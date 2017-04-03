package de.hpi.ingestion.dataimport.wikidata

import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.util.matching.Regex

class WikiDataRDDTest extends FlatSpec with SharedSparkContext with Matchers {
	var propertyMap: Map[String, String] = null
	var rdd: RDD[WikiDataRDD.WikiDataEntity] = null

	"WikiData entries" should "not be empty" in {
		getRDD() should not be empty
	}

	they should "have valid IDs" in {
		val validIDs = getRDD()
			.filter { entity =>
				val idRegex = new Regex("^(P|Q)[0-9]+$")
				idRegex.findFirstIn(entity.id) != None
			}

		validIDs.count should be (getRDD().count)
	}

	they should "have a label" in {
		val labelFound = getRDD()
			.filter { entity =>
				entity.label != "" && entity.label != null
			}

		labelFound.count should be (getRDD().count)
	}

	they should "be of type item or property" in {
		val invalidTypes = getRDD()
			.filter { entity =>
				entity.entitytype == "item" || entity.entitytype == "property"
			}

		invalidTypes.count should be (getRDD().count)
	}

	"WikiData Property map" should "not be empty" in {
		getPropertyMap() should not be empty
	}

	it should "contain only property IDs" in {
		var properties = getPropertyMap()
		val pidRegex = new Regex("^P[0-9]+$")
		var validIDs = properties.filter { case (id, label) =>
			pidRegex.findFirstIn(id) != None
		}

		validIDs.size should be (properties.size)
	}

	it should "have a label for each ID" in {
		var properties = getPropertyMap()
		var validLabels = properties.filter { case (id, label) =>
			label != "" && label != null
		}

		validLabels.size should be (properties.size)
	}

	"Translated WikiData entries" should "not contain any property IDs" in {
		val pidRegex = new Regex("^P[0-9]+$")
		var translated = WikiDataRDD
			.translateProps(getRDD(), sc.broadcast(getPropertyMap()))
			.filter(entity => {
				val translatedProps = entity.data.filter { case (id, value) =>
					pidRegex.findFirstIn(id) != None
				}

				entity.data.size == translatedProps.size
			})

		translated.count should be (getRDD().count)
	}

	def getRDD(): RDD[WikiDataRDD.WikiDataEntity] = {
		if(rdd == null) {
			rdd = WikiDataRDD.parseDump(wikiDataTestRDD())
		}
		rdd
	}

	def getPropertyMap(): Map[String, String] = {
		if(propertyMap == null) {
			propertyMap = WikiDataRDD.buildPropertyMap(WikiDataRDD.parseDump(wikiDataTestRDD()))
		}
		propertyMap
	}

	// extracted from WikiData dump
	def wikiDataTestRDD(): RDD[String] = {
		val lines = Source.fromURL(getClass.getResource("/wikidataTestData"))
			.getLines()
			.toList
		sc.parallelize(lines)
	}
}
