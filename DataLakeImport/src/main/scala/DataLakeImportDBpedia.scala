import DataLake.{DataLakeImport, Subject, SubjectManager, Version}
import scala.collection.mutable

object DataLakeImportDBpedia extends DataLakeImport(
	"DataLakeImportDBpedia_v1.0",
	List("dbpedia"),
	"wikidumps",
	"dbpedia"
){
	case class DBpediaEntity (
		var wikipageId: String = "null",
		var dbPediaName: String = "null",
		var label: Option[List[String]] = None,
		var description: Option[List[String]] = None,
		var data: Map[String, List[String]] = Map[String, List[String]]()
 	)

	override type T = DBpediaEntity

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
