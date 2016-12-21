import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Date, UUID}
import scala.collection.mutable
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs

object DataLakeImport {
	val appname = "DataLakeImport_v1.0"
	val datasources = List("wikidata_20161117")

	val keyspace = "wikidumps"
	val tablename = "wikidata"

	val outputKeyspace = "datalake"
	val outputTablename = "subject"
	val versionTablename = "version"

	case class WikiDataEntity(
		var id: String = "",
		var entitytype: Option[String] = None,
		var instancetype: Option[String] = None,
		var wikiname: Option[String] = None,
		var description: Option[String] = None,
		var label: Option[String] = None,
		var aliases: List[String] = List[String](),
		var data: Map[String, List[String]] = Map[String, List[String]]()
	)

	def translateToSubject(wd: WikiDataEntity, version: Version): Subject = {
		val subject = Subject()
		val sm = new SubjectManager(subject, version)

		if(wd.label.isDefined)
			sm.setName(wd.label.get)
		if(wd.aliases.size > 0)
			sm.addAliases(wd.aliases)
		if(wd.instancetype.isDefined)
			sm.setCategory(wd.instancetype.get)

		val metadata = mutable.Map(("wikidata_id", List(wd.id)))
		if(wd.wikiname.isDefined)
			metadata += "wikipedia_name" -> List(wd.wikiname.get)
		if(wd.data.size > 0)
			metadata ++= wd.data

		sm.addProperties(metadata.toMap)

		subject
	}

	def makeTemplateVersion(): Version = {
		// create timestamp and TimeUUID for versioning
		val timestamp = new Date()
		val version = UUIDs.timeBased()

		Version(version, appname, null, null, datasources, timestamp)
	}

	def main(args : Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName(appname)
			.set("spark.cassandra.connection.host", "172.20.21.11")

		val sc = new SparkContext(conf)
		val wikidata = sc.cassandraTable[WikiDataEntity](keyspace, tablename)

		val version = makeTemplateVersion()

		val lakeData = wikidata
			.filter(_.instancetype match {
				case Some(t) => true
				case None => false
			})
			.map(translateToSubject(_, version))
			.saveToCassandra(outputKeyspace, outputTablename)

		// write version information
		sc.parallelize(List((version.version, version.timestamp, version.datasources, version.program)))
			.saveToCassandra(outputKeyspace, versionTablename, SomeColumns("version", "timestamp", "datasources", "program"))

		//sc.stop
	}
}
