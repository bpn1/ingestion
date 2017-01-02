import DataLake.{DataLakeImport, Subject, SubjectManager, Version}
import scala.collection.mutable

object DataLakeImportDBpedia extends DataLakeImport[DBpediaEntity](
	"DataLakeImportDBpedia_v1.0",
	List("dbpedia"),
	"wikidumps",
	"dbpedia"
){
	override def translateToSubject(entity: DBpediaEntity, version: Version): Subject = {
		val subject = Subject()
		val sm = new SubjectManager(subject, version)

		if(entity.label.isDefined)
			sm.setName(entity.label.get.head)

		val metadata = mutable.Map[String, List[String]]("wikipageId" -> List(entity.wikipageId), "dbpediaName" -> List(entity.dbPediaName))
		if(entity.description.isDefined)
			metadata += "description" -> List(entity.description.get.head)
		if(entity.data.nonEmpty)
			metadata ++= entity.data

		sm.addProperties(metadata.toMap)
		subject
	}

	def main(args: Array[String]): Unit = {
		importToCassandra()
	}
}
