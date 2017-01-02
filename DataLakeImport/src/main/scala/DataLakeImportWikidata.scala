import DataLake.{DataLakeImport, Subject, SubjectManager, Version}
import scala.collection.mutable

object DataLakeImportWikidata extends DataLakeImport[WikiDataEntity](
	"DataLakeImportWikidata_v1.0",
	List("wikidata_20161117"),
	"wikidumps",
	"wikidata"
){
	override def translateToSubject(entity: WikiDataEntity, version: Version) : Subject = {
		val subject = Subject()
		val sm = new SubjectManager(subject, version)

		if(entity.label.isDefined)
			sm.setName(entity.label.get)
		if(entity.aliases.nonEmpty)
			sm.addAliases(entity.aliases)
		if(entity.instancetype.isDefined)
			sm.setCategory(entity.instancetype.get)

		val metadata = mutable.Map[String, List[String]](("wikidata_id", List(entity.id)))
		if(entity.wikiname.isDefined)
			metadata += "wikipedia_name" -> List(entity.wikiname.get)
		if(entity.data.nonEmpty)
			metadata ++= entity.data

		sm.addProperties(metadata.toMap)
		subject
	}

	def main(args: Array[String]): Unit = {
		importToCassandra()
	}
}
