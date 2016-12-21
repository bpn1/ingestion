import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import scala.collection.mutable

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

	def main(args : Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("TagEntities")
			.set("spark.cassandra.connection.host", "172.20.21.11")

		val sc = new SparkContext(conf)
		val wikidata = sc.cassandraTable(keyspace, tablename)

		val classData = wikidata
			.map(line => (line.get[String]("id"), line.get[Option[String]]("label"), line.get[Map[String,List[String]]]("data")))
			.map { case (id, label, data) =>
				(id, label.getOrElse(id), data.filterKeys(key => key == instanceProperty || key == subclassProperty))
			}

		val categoryData = classData
			.filter { case (id, label, data) =>
				data.contains(subclassProperty)
			}
			.cache

		// build subclass map for each given class
		val subclasses = mutable.Map[String, List[String]]()

		for((wikiDataID, tag) <- tagClasses) {
			var oldClasses = Map[String, List[String]](wikiDataID -> List(tag))
			var addedElements = true
			var newClasses = Map[String, List[String]]()

			while(addedElements) {
				// merge into subclasses based on path length
				for((key, path) <- oldClasses) {
					if(subclasses.contains(key)) {
						if(path.size < subclasses(key).size)
							subclasses(key) = path
					} else {
						subclasses(key) = path
					}
				}

				// recursively append next layer of tree
				newClasses = categoryData
					.map { case (id, label, data) =>
						(id, label, data(subclassProperty).filter(oldClasses.contains(_)))
					}.filter { case (id, label, classList) =>
						classList.size > 0
					}.map { case (id, label, classList) =>
						(id, oldClasses(classList.head) ++ List(label))
					}.collect.toMap

				// check if new elements were added and not stuck in a loop
				addedElements = newClasses.size > 0 && !(newClasses.size == oldClasses.size && newClasses.keySet == oldClasses.keySet)

				oldClasses = newClasses
				println(s"Tag: $tag, New size: ${newClasses.size}")
			}
		}

		// save subclasses as text
		sc.parallelize(subclasses.toList)
			.map { case (id, path) => id + " => " + path.mkString("/") }
			.saveAsTextFile("subclass_tree_" + System.currentTimeMillis / 1000)

		// update all entities that fit the found classes
		val updatedEntities = classData
			.filter { case (id, label, data) =>
				if(data.contains(instanceProperty))
					data(instanceProperty).filter(subclasses.contains(_)).size > 0
				else
					false
			}.map { case (id, label, data) =>
				val path = subclasses(data(instanceProperty)
					.filter(subclasses.contains(_)).head)

				(id, path.head, Map(wikidataPathProperty -> path))
			}

		updatedEntities.saveToCassandra(keyspace, tablename, SomeColumns("id", "instancetype", "data" append))
	}
}
