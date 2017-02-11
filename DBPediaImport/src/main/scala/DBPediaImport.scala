import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

object DBPediaImport {
	val appName = "DBPediaImport_v1.0"
	val dataSources = List("dbpedia_20161203")

	val keyspace = "wikidumps"
	val tablename = "dbpedia"

	case class DBPediaTriple (subject: String, predicate: String, property: String)

	def tokenize(turtle: String) : Array[String] = {
		turtle
			.split("> ")
			.map(_.trim)
			.filter(_ != ".")
	}

	def cleanURL(str:String, prefixes: List[(String,String)]) : String = {
		var res = str.replaceAll("""[<>\"]""", "")
		for (pair <- prefixes) {
			res = res.replace(pair._1, pair._2)
		}
		res
	}

	def parseLine(text: String, prefixes: List[(String,String)]) : DBPediaTriple = {
		val triple = tokenize(text).map(cleanURL(_, prefixes))
		DBPediaTriple(triple(0), triple(1), triple(2))
	}

	def parseTurtleFile(rdd: RDD[String], prefixes: List[(String,String)]) : RDD[DBPediaTriple] = {
		rdd.map(parseLine(_, prefixes))
	}

	def extractProperties(group: Tuple2[String, Iterable[DBPediaTriple]]) : Iterable[Tuple2[String, String]] = group match {
		case (subject, triples) => triples.map(triple => Tuple2(triple.predicate, triple.property)) ++ List(("dbpedia-entity", subject))
	}

	def createMap(tupels: Iterable[Tuple2[String, String]]) : Map[String, Iterable[String]] = {
		tupels
			// List( ("type","human"),("type","animal"),("name","odin"),... ) -> Map("type" -> List(("type, human), ("type", "animal)), "name" -> List(...)
			.groupBy {
				case (predicate, property) => predicate
			}
			// Map("type" -> List(("type, human), ("type", "animal)), "name" -> List(...) -> Map("type" -> List("human","animal"), "name" -> List("odin), ...)
			.mapValues(_.map {
				case (predicate, property) => property
			})
		  .map(identity)
	}

	def translateToDBPediaEntry(resource: Map[String, Iterable[String]]) : DBPediaEntity = {
		val dBPediaEntity = DBPediaEntity()
		dBPediaEntity.wikipageId = resource.getOrElse("dbo:wikiPageID", List("null")).head
		dBPediaEntity.dbPediaName = resource.getOrElse("dbpedia-entity", List("null")).head
		dBPediaEntity.label = resource.get("rdfs:label")
		dBPediaEntity.description = resource.get("dbo:abstract")
		dBPediaEntity.instancetype = resource.get("rdf:type")
		dBPediaEntity.data = resource - ("dbo:wikiPageID", "dbpedia-entity", "rdfs:label", "dbo:abstract")
		dBPediaEntity
	}

	def createDBpediaEntities(rdd: RDD[DBPediaTriple]) : RDD[DBPediaEntity] = {
		rdd
			.groupBy(_.subject)
			.map(extractProperties)
			.map(createMap)
			.map(translateToDBPediaEntry)
	}

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName(appName)
			.set("spark.cassandra.connection.host", "odin01")
		val sc = new SparkContext(conf)

		val prefixes = sc
			.textFile("prefixes.txt")
			.map(_.trim.replaceAll("""[()]""", "").split(","))
			.map(pair => (pair(0), pair(1)))
			.collect
			.toList

		val prefixesBroadcast = sc.broadcast(prefixes)

		val ttl = sc.textFile("dbpedia_de_clean.ttl")  // original file
		val triples = parseTurtleFile(ttl, prefixesBroadcast.value)

		val dbpediaResources = triples.filter(resource => resource.subject.contains("dbr:") || resource.subject.contains("dbpedia-de:"))
		val dbpediaEntities = createDBpediaEntities(dbpediaResources)
		dbpediaEntities.saveToCassandra(keyspace, tablename)

		sc.stop()
	}
}
