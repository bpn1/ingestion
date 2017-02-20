import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import com.datastax.spark.connector._
import scala.util.matching.Regex

object ResolveEntities {
	val keyspace = "wikidumps"
	val tablename = "wikidata"

	def main(args : Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("ResolveEntities")
			.set("spark.cassandra.connection.host", "odin01")

		val sc = new SparkContext(conf)
		val wikidata = sc.cassandraTable(keyspace, tablename)
			.map(line => (line.get[String]("id"), line.get[Option[String]]("label"),
				line.get[Map[String,List[String]]]("data"), line.get[Option[String]]("instancetype")))

		// flatten (id, data) tupels to (id, property, value) tuples
		val data = wikidata
			.map { case (id, label, data, instancetype) =>
				data
					.map { case (property, valueList) =>
						valueList.map(element => (property, element))
					}
					.flatMap(tuple => tuple)
					.map(tuple => (id, tuple._1, tuple._2))
			}
			.flatMap(tuple => tuple)
			.cache

		// filter all entries not having a wikidata entity as value
		val regex = new Regex("^(P|Q)[0-9]+$")
		val noIdData = data
			.filter { case (id, property, value) =>
				regex.findFirstIn(value) == None
			}

		// filter all entries which do not need to be resolved
		val unitRegex = new Regex(";(P|Q)[0-9]+$")
		val noEntityData = noIdData
			.filter { case (id, property, value) =>
				unitRegex.findFirstIn(value) == None
			}

		// filter all entries with a wikidata entity as unit of measurement and transform to joinable RDD
		val unitData = noIdData
			.filter { case (id, property, value) =>
				unitRegex.findFirstIn(value) != None
			}
			.map { case (id, property, value) => {
					val valueArray = value.split(";")
					(id, property, valueArray(0), valueArray(1))
				}
			}
			.keyBy(_._4)

		// transform to joinable RDD
		val idData = data
			.filter { case (id, property, value) =>
				regex.findFirstIn(value) != None
			}
			.keyBy(_._3)

		// transform to joinable RDD
		val names = wikidata
			.filter { case (id, label, data, instancetype) =>
				label != None
			}
			.filter {
				case (id, label, data, Some(instancetype)) =>
					false
				case (id, label, data, None) =>
					true
			}
			.map { case (id, label, data, instancetype) =>
				(id, label.get)
			}
			.keyBy(_._1)

		// join RDDS with names and reduce result
		val idJoin = idData
			.leftOuterJoin(names)
			.map {
				case (joinId, ((id, property, value), Some((entityId, label)))) =>
					(id, property, label)
				case (joinId, ((id, property, value), None)) =>
					(id, property, value)
			}

		val unitJoin = unitData
			.leftOuterJoin(names)
			.map {
				case (joinId, ((id, property, value, valueUnit), Some((entityId, label)))) =>
					(id, property, value + ";" + label)
				case (joinId, ((id, property, value, valueUnit), None)) =>
					(id, property, value + ";" + valueUnit)
			}

		// concatenate all RDDs
		val resolvedData = idJoin
			.union(unitJoin)
			.union(noEntityData)

		// create nested data structure for data attribute
		resolvedData
			.map { case (id, property, value) =>
				(id, (property, value))
			}
			.groupByKey
			.map { case (id, propertyList) => {
					val propertyMap = propertyList
						.groupBy(_._1)
						.mapValues(_.map(_._2))
					(id, propertyMap)
				}
			}
			.saveToCassandra("wikidumps", "wikidata", SomeColumns("id", "data"))
		sc.stop
	}
}
