import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import com.datastax.spark.connector._
import scala.util.matching.Regex

object ResolveEntities {
	val keyspace = "wikidumps"
	val tablename = "wikidata"


	def filterIdRDD(
		data: RDD[(String, String, String)],
		entityRegex: Regex
	): RDD[(String, String, String)] =
	{
		// filter all entries not having a wikidata entity as value
		data.filter { case (id, property, value) =>
			entityRegex.findFirstIn(value).isEmpty
		}

	}

	def flattenWikidata(
		wikidata: RDD[(String, Option[String], Map[String,List[String]], Option[String])]
	): RDD[(String, String, String)] =
	{
		// flatten (id, data) tupels to (id, property, value) tuples
		wikidata.flatMap { case (id, label, data, instancetype) =>
				data
					.flatMap { case (property, valueList) =>
						valueList.map(element => (property, element))
					}.map(tuple => (id, tuple._1, tuple._2))
			}
	}

	def noResolveRDD(
		dataRDD: RDD[(String, String, String)],
		unitRegex: Regex
	): RDD[(String, String, String)] =
	{
		// filter all entries which do not need to be resolved
		dataRDD
			.filter { case (id, property, value) => unitRegex.findFirstIn(value).isEmpty }
	}

	def unitDataRDD(
		dataRDD: RDD[(String, String, String)],
		unitRegex: Regex
	): RDD[(String, (String, String, String, String))] =
	{
		// filter all entries with a wikidata entity as unit of measurement
		// then transform to joinable RDD
		dataRDD
			.filter { case (id, property, value) => unitRegex.findFirstIn(value).isDefined }
			.map { case (id, property, value) => {
					val valueArray = value.split(";")
					(id, property, valueArray(0), valueArray(1))
				}
			}.keyBy(_._4)
	}

	def resolveNamesRDD(
		dataRDD: RDD[(String, Option[String], Map[String,List[String]], Option[String])]
	): RDD[(String, (String, String))] =
	{
		// transform to joinable RDD
		dataRDD
			.filter { case (id, label, data, instancetype) => label.isDefined }
			.filter { case (id, label, data, instancetypeOption) => instancetypeOption.isEmpty }
			.map { case (id, label, data, instancetype) => (id, label.get) }
			.keyBy(_._1)
	}

	def joinIdRDD(
		idData: RDD[(String, (String, String, String))],
		nameData: RDD[(String, (String, String))]
	): RDD[(String, String, String)] =
	{
		// join RDD with names and reduce result
		idData
			.leftOuterJoin(nameData)
			.map {
				case (joinId, ((id, property, value), Some((entityId, label)))) =>
					(id, property, label)
				case (joinId, ((id, property, value), None)) =>
					(id, property, value)
			}
	}

	def joinUnitRDD(
		unitData: RDD[(String, (String, String, String, String))],
		nameData: RDD[(String, (String, String))]
	): RDD[(String, String, String)] =
	{
		unitData
			.leftOuterJoin(nameData)
			.map {
				case (joinId, ((id, property, value, valueUnit), Some((entityId, label)))) =>
					(id, property, value + ";" + label)
				case (joinId, ((id, property, value, valueUnit), None)) =>
					(id, property, value + ";" + valueUnit)
			}
	}

	def main(args : Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("ResolveEntities")
			.set("spark.cassandra.connection.host", "odin01")

		val entityRegex = new Regex("^(P|Q)[0-9]+$")
		val unitRegex = new Regex(";(P|Q)[0-9]+$")
		val sc = new SparkContext(conf)
		val wikidata = sc.cassandraTable(keyspace, tablename)
			.map(line =>
					(line.get[String]("id"),
						line.get[Option[String]]("label"),
						line.get[Map[String,List[String]]]("data"),
						line.get[Option[String]]("instancetype")))

		val data = flattenWikidata(wikidata).cache
		val noIdData = filterIdRDD(data, entityRegex)
		val noEntityData = noResolveRDD(noIdData, unitRegex)
		val unitData = unitDataRDD(noIdData, unitRegex)
		val names = resolveNamesRDD(wikidata)

		// transform to joinable RDD
		val idData = data
			.filter { case (id, property, value) => entityRegex.findFirstIn(value).isDefined }
			.keyBy(_._3)

		val idJoin = joinIdRDD(idData, names)
		val unitJoin = joinUnitRDD(unitData, names)

		// concatenate all RDDs
		val resolvedData = idJoin
			.union(unitJoin)
			.union(noEntityData)

		// create nested data structure for data attribute
		resolvedData
			.map { case (id, property, value) => (id, List((property, value))) }
			.reduceByKey(_ ++ _)
			.map { case (id, propertyList) => {
					val propertyMap = propertyList
						.groupBy(_._1)
						.mapValues(_.map(_._2))
					(id, propertyMap)
				}
			}.saveToCassandra("wikidumps", "wikidata", SomeColumns("id", "data"))
		sc.stop
	}
}
