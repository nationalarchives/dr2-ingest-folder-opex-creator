package uk.gov.nationalarchives

import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import uk.gov.nationalarchives.Lambda.{Asset, DynamoTable, ArchiveFolder, ContentFolder}

import java.util.UUID
import scala.xml.{Elem, PrettyPrinter}

class XMLCreatorTest extends AnyFlatSpec {

  val expectedStandardXml: Elem = <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
    <opex:Properties>
      <opex:Title>title</opex:Title>
      <opex:Description>description</opex:Description>
      <opex:SecurityDescriptor>open</opex:SecurityDescriptor>
      <Identifers>
        <Identifer type="Code">name</Identifer>
      </Identifers>
    </opex:Properties>
    <opex:Transfer>
      <opex:SourceID>name</opex:SourceID>
      <opex:Manifest>
        <opex:Folders>
          <opex:Folder>a814ee41-89f4-4975-8f92-303553fe9a02.pax</opex:Folder>
          <opex:Folder>9ecbba86-437f-42c6-aeba-e28b678bbf4c.pax</opex:Folder>
          <opex:Folder>7fcd94a9-be3f-456d-875f-bc697f7ed106</opex:Folder>
          <opex:Folder>9ecbba86-437f-42c6-aeba-e28b678bbf4c</opex:Folder>
        </opex:Folders>
        <opex:Files>
          <opex:File type="metadata" size="1">a814ee41-89f4-4975-8f92-303553fe9a02.pax.opex</opex:File>
          <opex:File type="metadata" size="1">9ecbba86-437f-42c6-aeba-e28b678bbf4c.pax.opex</opex:File>
        </opex:Files>
      </opex:Manifest>
    </opex:Transfer>
  </opex:OPEXMetadata>

  val expectedXmlNoTitle: Elem = <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
    <opex:Properties>
      <opex:Title>name</opex:Title>
      <opex:Description>description</opex:Description>
      <opex:SecurityDescriptor>open</opex:SecurityDescriptor>
      <Identifers></Identifers>
    </opex:Properties>
    <opex:Transfer>
      <opex:Manifest>
        <opex:Folders>
          <opex:Folder>a814ee41-89f4-4975-8f92-303553fe9a02.pax</opex:Folder>
          <opex:Folder>9ecbba86-437f-42c6-aeba-e28b678bbf4c.pax</opex:Folder>
          <opex:Folder>7fcd94a9-be3f-456d-875f-bc697f7ed106</opex:Folder>
          <opex:Folder>9ecbba86-437f-42c6-aeba-e28b678bbf4c</opex:Folder>
        </opex:Folders>
        <opex:Files>
          <opex:File type="metadata" size="1">a814ee41-89f4-4975-8f92-303553fe9a02.pax.opex</opex:File>
          <opex:File type="metadata" size="1">9ecbba86-437f-42c6-aeba-e28b678bbf4c.pax.opex</opex:File>
        </opex:Files>
      </opex:Manifest>
    </opex:Transfer>
  </opex:OPEXMetadata>

  val expectedXMLNoHierarchyFolder: Elem = <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
    <opex:Properties>
      <opex:Title>name</opex:Title>
      <opex:Description>description</opex:Description>
      <opex:SecurityDescriptor>open</opex:SecurityDescriptor>
      <Identifers></Identifers>
    </opex:Properties>
    <opex:Transfer>
      <opex:Manifest>
        <opex:Folders>
          <opex:Folder>a814ee41-89f4-4975-8f92-303553fe9a02.pax</opex:Folder>
          <opex:Folder>9ecbba86-437f-42c6-aeba-e28b678bbf4c.pax</opex:Folder>
          <opex:Folder>7fcd94a9-be3f-456d-875f-bc697f7ed106</opex:Folder>
          <opex:Folder>9ecbba86-437f-42c6-aeba-e28b678bbf4c</opex:Folder>
        </opex:Folders>
        <opex:Files>
          <opex:File type="metadata" size="1">a814ee41-89f4-4975-8f92-303553fe9a02.pax.opex</opex:File>
          <opex:File type="metadata" size="1">9ecbba86-437f-42c6-aeba-e28b678bbf4c.pax.opex</opex:File>
        </opex:Files>
      </opex:Manifest>
    </opex:Transfer>
  </opex:OPEXMetadata>

  val folder: DynamoTable = DynamoTable(
    "TEST-ID",
    UUID.fromString("90730c77-8faa-4dbf-b20d-bba1046dac87"),
    "parentPath",
    "name",
    ArchiveFolder,
    "title",
    "description",
    Option(1),
    Option("checksum"),
    Option("ext")
  )
  val assetUuids: List[UUID] = List(UUID.fromString("a814ee41-89f4-4975-8f92-303553fe9a02"), UUID.fromString("9ecbba86-437f-42c6-aeba-e28b678bbf4c"))
  val folderUuids: List[UUID] = List(UUID.fromString("7fcd94a9-be3f-456d-875f-bc697f7ed106"), UUID.fromString("9ecbba86-437f-42c6-aeba-e28b678bbf4c"))
  val childAssets: List[DynamoTable] = assetUuids.zipWithIndex.map { case (uuid, suffix) =>
    DynamoTable(
      "TEST-ID",
      uuid,
      s"parentPath$suffix",
      s"name$suffix Asset",
      Asset,
      s"title$suffix Asset",
      s"description$suffix Asset",
      Option(1),
      Option(s"checksum$suffix Asset"),
      Option(s"ext$suffix")
    )
  }

  val childFolders: List[DynamoTable] = folderUuids.zipWithIndex.map { case (uuid, suffix) =>
    DynamoTable(
      "TEST-ID",
      uuid,
      s"parentPath$suffix",
      s"name$suffix Folder",
      ContentFolder,
      s"title$suffix Folder",
      s"description$suffix Folder",
      Option(1),
      Option(s"checksum$suffix Folder"),
      Option(s"ext$suffix")
    )
  }

  "createOpex" should "create the correct opex xml" in {
    val xml = XMLCreator().createFolderOpex(folder, childAssets, childFolders).unsafeRunSync()
    val prettyPrinter = new PrettyPrinter(200, 2)
    prettyPrinter.format(expectedStandardXml) should equal(xml)
  }

  "createOpex" should "use the name if the title is blank" in {
    val xml = XMLCreator().createFolderOpex(folder.copy(title = ""), childAssets, childFolders).unsafeRunSync()
    val prettyPrinter = new PrettyPrinter(200, 2)
    prettyPrinter.format(expectedXmlNoTitle) should equal(xml)
  }

  "createOpex" should "exclude the SourceId and Identifier if the name title are the same" in {
    val xml = XMLCreator().createFolderOpex(folder.copy(title = "name"), childAssets, childFolders).unsafeRunSync()
    val prettyPrinter = new PrettyPrinter(200, 2)
    prettyPrinter.format(expectedXMLNoHierarchyFolder) should equal(xml)
  }
}
