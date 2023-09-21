package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import org.scanamo.{DynamoFormat, TypeCoercionError}
import org.scanamo.generic.semiauto._
import org.scanamo.syntax._
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.DADynamoDBClient._
import uk.gov.nationalarchives.Lambda._
import upickle.default._
import fs2.Stream
import fs2.interop.reactivestreams._
import org.scanamo.generic.auto.Typeclass

import java.io.{InputStream, OutputStream}
import java.util.UUID

class Lambda extends RequestStreamHandler {
  implicit val typeFormat: Typeclass[Type] = DynamoFormat.xmap[Type, String](
    {
      case "Folder"   => Right(Folder)
      case "Asset"    => Right(Asset)
      case "File"     => Right(File)
      case typeString => Left(TypeCoercionError(new Exception(s"Type $typeString not found")))
    },
    typeObject => typeObject.toString
  )

  implicit val dynamoTableFormat: Typeclass[DynamoTable] = deriveDynamoFormat[DynamoTable]
  implicit val pkFormat: Typeclass[PartitionKey] = deriveDynamoFormat[PartitionKey]

  private val xmlCreator: XMLCreator = XMLCreator()
  val dynamoClient: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  val s3Client: DAS3Client[IO] = DAS3Client[IO]()

  override def handleRequest(inputStream: InputStream, output: OutputStream, context: Context): Unit = {
    val inputString = inputStream.readAllBytes().map(_.toChar).mkString
    val input = read[Input](inputString)
    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      folderItems <- dynamoClient.getItems[DynamoTable, PartitionKey](List(PartitionKey(input.id)), config.dynamoTableName)
      folder <- IO.fromOption(folderItems.headOption)(
        new Exception(s"No folder found for ${input.id} and ${input.batchId}")
      )
      _ <- if (folder.`type` != Folder) IO.raiseError(new Exception(s"Object ${folder.id} is of type ${folder.`type`} and not 'folder'")) else IO.unit
      children <- childrenOfFolder(folder, config.dynamoTableName, config.dynamoGsiName)
      _ <- IO.fromOption(children.headOption)(new Exception(s"No children found for ${input.id} and ${input.batchId}"))
      assetRows <- getAssetRowsWithFileSize(children, config.bucketName, input.executionName)
      folderRows <- IO(children.filter(_.`type` == Folder))
      folderOpex <- xmlCreator.createFolderOpex(folder, assetRows, folderRows)
      _ <- uploadXMLToS3(folderOpex, config.bucketName, generateKey(input.executionName, folder))
    } yield ()
  }.unsafeRunSync()

  private def generateKey(executionName: String, folder: DynamoTable) =
    s"opex/$executionName/${formatParentPath(folder.parentPath)}${folder.id}/${folder.id}.opex"

  private def formatParentPath(parentPath: String): String = if (parentPath.isBlank) "" else s"$parentPath/"

  private def uploadXMLToS3(xmlString: String, destinationBucket: String, key: String): IO[CompletedUpload] =
    Stream.emits[IO, Byte](xmlString.getBytes).chunks.map(_.toByteBuffer).toUnicastPublisher.use { publisher =>
      s3Client.upload(destinationBucket, key, xmlString.getBytes.length, publisher)
    }

  private def getAssetRowsWithFileSize(children: List[DynamoTable], bucketName: String, executionName: String): IO[List[DynamoTable]] = {
    children
      .filter(_.`type` == Asset)
      .map { asset =>
        val key = s"opex/$executionName/${asset.parentPath}/${asset.id}.pax.opex"
        s3Client
          .headObject(bucketName, key)
          .map(headResponse => asset.copy(fileSize = Option(headResponse.contentLength())))
      }
      .sequence
  }

  private def childrenOfFolder(asset: DynamoTable, tableName: String, gsiName: String): IO[List[DynamoTable]] = {
    val childrenParentPath = s"${asset.parentPath}/${asset.id}".stripPrefix("/")
    dynamoClient
      .queryItems[DynamoTable](tableName, gsiName, "batchId" === asset.batchId and "parentPath" === childrenParentPath)
  }
}

object Lambda {
  implicit val inputReader: Reader[Input] = macroR[Input]

  private case class Config(dynamoTableName: String, bucketName: String, dynamoGsiName: String)

  case class Input(id: UUID, batchId: String, executionName: String)

  sealed trait Type {
    override def toString: String = this match {
      case Folder => "Folder"
      case Asset  => "Asset"
      case File   => "File"
    }
  }

  case object Folder extends Type

  case object Asset extends Type

  case object File extends Type

  case class DynamoTable(
      batchId: String,
      id: UUID,
      parentPath: String,
      name: String,
      `type`: Type,
      title: String,
      description: String,
      fileSize: Option[Long] = None,
      checksumSha256: Option[String] = None,
      fileExtension: Option[String] = None
  )

  case class PartitionKey(id: UUID)
}
