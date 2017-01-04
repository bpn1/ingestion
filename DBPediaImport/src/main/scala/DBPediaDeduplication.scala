import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import org.apache.spark.sql.SQLContext

object DBPediaDeduplication {

	val appname = "DBPediaImport_v1.0"
	val dataSources = List("dbpedia_20161203")

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName(appname)
			.set("spark.cassandra.connection.host", "172.20.21.11")

		val sc = new SparkContext(conf)
		val sql = new SQLContext(sc)

		val dbpedia = sc.cassandraTable[DBPediaEntity]("wikidumps", "dbpeidadata")
		val subjects = sc.cassandraTable[Subject]("datalake", "subject")

		val pairDBpedia = dbpedia.keyBy(_.wikipageId)
		val pairSubject = subjects
		  .filter(subject => subject.properties.get("wikipageId").isDefined)
		  .keyBy(_.properties.getOrElse("wikipageId", List("null")).head)
		//val joinedTable = pairDBpedia.leftOuterJoin(pairSubject)
		//joinedTable.takeSample(false, 10).foreach(println)

		sc.stop()
	}
}

