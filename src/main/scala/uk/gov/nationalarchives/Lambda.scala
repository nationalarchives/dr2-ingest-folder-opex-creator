package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import fs2.Stream
import fs2.interop.reactivestreams._
import org.scanamo.{DynamoFormat, DynamoReadError, DynamoValue, MissingProperty, TypeCoercionError}
import org.scanamo.syntax._
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.DynamoFormatters._
import uk.gov.nationalarchives.DADynamoDBClient._
import uk.gov.nationalarchives.Lambda._
import upickle.default._

import java.io.{InputStream, OutputStream}
import java.util.UUID
import scala.jdk.CollectionConverters.MapHasAsScala

class Lambda extends RequestStreamHandler {

  private val xmlCreator: XMLCreator = XMLCreator()
  val dynamoClient: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  val s3Client: DAS3Client[IO] = DAS3Client[IO]()

  private def toFolderOrAssetTable[T <: DynamoTable](dynamoValue: DynamoValue)(implicit dynamoFormat: DynamoFormat[T]): Either[DynamoReadError, FolderOrAssetTable] =
    dynamoFormat.read(dynamoValue).map { table =>
      FolderOrAssetTable(table.batchId, table.id, table.parentPath, table.name, table.`type`, table.title, table.description, table.identifiers)
    }

  implicit val folderOrAssetFormat: DynamoFormat[FolderOrAssetTable] = new DynamoFormat[FolderOrAssetTable] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, FolderOrAssetTable] =
      dynamoValue.toAttributeValue.m().asScala.toMap.get("type").map(_.s()) match {
        case Some(rowType) =>
          rowType match {
            case "Asset"                           => toFolderOrAssetTable[AssetDynamoTable](dynamoValue)
            case "ArchiveFolder" | "ContentFolder" => toFolderOrAssetTable[ArchiveFolderDynamoTable](dynamoValue)
            case _                                 => Left(TypeCoercionError(new RuntimeException("Row is not an 'Asset' or a 'Folder'")))
          }
        case None => Left[DynamoReadError, FolderOrAssetTable](MissingProperty)
      }

    // We're not using write but we have to have this overridden
    override def write(t: FolderOrAssetTable): DynamoValue = DynamoValue.nil
  }

  override def handleRequest(inputStream: InputStream, output: OutputStream, context: Context): Unit = {
    val inputString = inputStream.readAllBytes().map(_.toChar).mkString
    val input = read[Input](inputString)
    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      folderItems <- dynamoClient.getItems[ArchiveFolderDynamoTable, PartitionKey](List(PartitionKey(input.id)), config.dynamoTableName)
      folder <- IO.fromOption(folderItems.headOption)(
        new Exception(s"No folder found for ${input.id} and ${input.batchId}")
      )
      _ <- if (!isFolder(folder.`type`)) IO.raiseError(new Exception(s"Object ${folder.id} is of type ${folder.`type`} and not 'ContentFolder' or 'ArchiveFolder'")) else IO.unit
      children <- childrenOfFolder(folder, config.dynamoTableName, config.dynamoGsiName)
      _ <- IO.fromOption(children.headOption)(new Exception(s"No children found for ${input.id} and ${input.batchId}"))
      assetRows <- getAssetRowsWithFileSize(children, config.bucketName, input.executionName)
      folderRows <- IO(children.filter(child => isFolder(child.`type`)))
      folderOpex <- xmlCreator.createFolderOpex(folder, assetRows, folderRows, folder.identifiers)
      _ <- uploadXMLToS3(folderOpex, config.bucketName, generateKey(input.executionName, folder))
    } yield ()
  }.unsafeRunSync()

  private def isFolder(rowType: Type) = List(ContentFolder, ArchiveFolder).contains(rowType)

  private def generateKey(executionName: String, folder: DynamoTable) =
    s"opex/$executionName/${formatParentPath(folder.parentPath)}${folder.id}/${folder.id}.opex"

  private def formatParentPath(potentialParentPath: Option[String]): String = potentialParentPath.map(parentPath => s"$parentPath/").getOrElse("")

  private def uploadXMLToS3(xmlString: String, destinationBucket: String, key: String): IO[CompletedUpload] =
    Stream.emits[IO, Byte](xmlString.getBytes).chunks.map(_.toByteBuffer).toUnicastPublisher.use { publisher =>
      s3Client.upload(destinationBucket, key, xmlString.getBytes.length, publisher)
    }

  private def getAssetRowsWithFileSize(children: List[FolderOrAssetTable], bucketName: String, executionName: String): IO[List[AssetWithFileSize]] = {
    children.collect {
      case child @ asset if child.`type` == Asset =>
        val key = s"opex/$executionName/${formatParentPath(asset.parentPath)}${asset.id}.pax.opex"
        s3Client
          .headObject(bucketName, key)
          .map(headResponse => AssetWithFileSize(asset, headResponse.contentLength()))
    }.sequence
  }

  private def childrenOfFolder(asset: ArchiveFolderDynamoTable, tableName: String, gsiName: String): IO[List[FolderOrAssetTable]] = {
    val childrenParentPath = s"${asset.parentPath.getOrElse("")}/${asset.id}".stripPrefix("/")
    dynamoClient
      .queryItems[FolderOrAssetTable](tableName, gsiName, "batchId" === asset.batchId and "parentPath" === childrenParentPath)
  }
}

object Lambda {
  implicit val inputReader: Reader[Input] = macroR[Input]

  private case class Config(dynamoTableName: String, bucketName: String, dynamoGsiName: String)

  case class Input(id: UUID, batchId: String, executionName: String)

  case class AssetWithFileSize(asset: FolderOrAssetTable, fileSize: Long)

  case class FolderOrAssetTable(
      batchId: String,
      id: UUID,
      parentPath: Option[String],
      name: String,
      `type`: Type,
      title: Option[String],
      description: Option[String],
      identifiers: List[Identifier]
  )

}
