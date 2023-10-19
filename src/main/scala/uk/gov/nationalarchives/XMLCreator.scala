package uk.gov.nationalarchives

import cats.effect.IO
import uk.gov.nationalarchives.DynamoFormatters._

import scala.xml.PrettyPrinter

class XMLCreator {

  def createFolderOpex(
      folder: DynamoTable,
      childAssets: List[DynamoTable],
      childFolders: List[DynamoTable],
      identifiers: List[Identifier],
      securityDescriptor: String = "open"
  ): IO[String] = IO {
    val isHierarchyFolder: Boolean = folder.`type` == ArchiveFolder
    val prettyPrinter = new PrettyPrinter(180, 2)
    val xml = <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
      <opex:Properties>
        <opex:Title>{folder.title.getOrElse(folder.name)}</opex:Title>
        <opex:Description>{folder.description.getOrElse("")}</opex:Description>
        <opex:SecurityDescriptor>{securityDescriptor}</opex:SecurityDescriptor>
        {
      if (identifiers.nonEmpty) {
        <Identifiers>
          {identifiers.map(identifier => <Identifier type={identifier.identifierName}>{identifier.value}</Identifier>)}
        </Identifiers>
      }
    }
      </opex:Properties>
      <opex:Transfer>
        {if (isHierarchyFolder) <opex:SourceID>{folder.name}</opex:SourceID>}
        <opex:Manifest>
          <opex:Folders>
            {childAssets.map(asset => <opex:Folder>{asset.id}.pax</opex:Folder>)}
            {childFolders.map(folder => <opex:Folder>{folder.id}</opex:Folder>)}
          </opex:Folders>
          <opex:Files>
            {childAssets.map(asset => <opex:File type="metadata" size={asset.fileSize.getOrElse(0).toString}>{asset.id.toString}.pax.opex</opex:File>)}
          </opex:Files>
        </opex:Manifest>
      </opex:Transfer>
    </opex:OPEXMetadata>
    prettyPrinter.format(xml)
  }
}
object XMLCreator {
  def apply(): XMLCreator = new XMLCreator()
}
