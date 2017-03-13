import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import scala.collection.mutable
import org.apache.spark.rdd._

object TagEntities {
	val keyspace = "wikidumps"
	val tablename = "wikidata"

	val tagClasses = Map(
		"Q43229" -> "organization",
		"Q4830453" -> "business",
		//"Q215627" -> "person",
		//"Q1496967" -> "location", // is "territorial entity" (previously "Q2221906")
		"Q268592" -> "economic branch"
	)

	val instanceProperty = "instance of"
	val subclassProperty = "subclass of"
	val wikidataPathProperty = "wikidata_path"

	case class SubclassEntry(
		id: String,
		var label: String = "",
		var data: Map[String, List[String]] = Map[String, List[String]](),
		var classList: List[String] = List[String]())

	def addShorterPaths(
		subclasses: Map[String, List[String]],
		oldClasses: Map[String, List[String]]
	): Map[String, List[String]] = {
		// merge into subclasses based on path length
		val resultMap = mutable.Map[String, List[String]]()
		resultMap ++= subclasses
		oldClasses.foreach { case (key, path) =>
			val oldPathLength = resultMap.get(key).map(_.size)
			if(oldPathLength.isEmpty || path.size < oldPathLength.get) {
				resultMap(key) = path
			}
		}
		resultMap.toMap
	}

	def buildSubclassMap(
		categoryData: RDD[SubclassEntry]
	): Map[String, List[String]] = {
		// build subclass map for each given class
		val subclasses = mutable.Map[String, List[String]]()

		for((wikiDataID, tag) <- tagClasses) {
			var oldClasses = Map(wikiDataID -> List(tag))
			var addedElements = true
			var newClasses = Map[String, List[String]]()

			while(addedElements) {
				subclasses ++= addShorterPaths(subclasses.toMap, oldClasses)

				// recursively append next layer of tree
				newClasses = categoryData
					.map { entry =>
						entry.classList = entry.data(subclassProperty)
							.filter(oldClasses.contains)
						entry
					}.filter { entry => entry.classList.nonEmpty }
					.map { entry =>
						(entry.id, oldClasses(entry.classList.head) ++ List(entry.label))
					}.collect
					.toMap

				// check if new elements were added and not stuck in a loop
				addedElements = newClasses.nonEmpty &&
					!(newClasses.size == oldClasses.size &&
						newClasses.keySet == oldClasses.keySet)

				oldClasses = newClasses
			}
		}
		subclasses.toMap
	}

	def main(args : Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("TagEntities")
			.set("spark.cassandra.connection.host", "odin01")

		val sc = new SparkContext(conf)
		val classData = sc.cassandraTable[WikiDataEntity](keyspace, tablename)
			.map { entry =>
				SubclassEntry(entry.id,
					entry.label.getOrElse(entry.id),
					entry.data
						.filterKeys(key => key == instanceProperty || key == subclassProperty))
			}

		val categoryData = classData
			.filter { subclassEntry =>
				subclassEntry.data.contains(subclassProperty)
			}.cache

		val subclasses = buildSubclassMap(categoryData)

		// save subclasses as text
		sc.parallelize(subclasses.toList)
			.map { case (id, path) => id + " => " + path.mkString("/") }
			.saveAsTextFile("subclass_tree_" + System.currentTimeMillis / 1000)

		// update all entities that fit the found classes
		val updatedEntities = classData
			.filter(_.data
				.getOrElse(instanceProperty, List[String]())
				.exists(subclasses.contains))
			.map { entry =>
				val path = subclasses(entry.data(instanceProperty)
					.filter(subclasses.contains)
					.head)
				(entry.id, path.head, Map(wikidataPathProperty -> path))
			}

		updatedEntities.saveToCassandra(
			keyspace,
			tablename,
			SomeColumns("id", "instancetype", "data" append))
	}
}
