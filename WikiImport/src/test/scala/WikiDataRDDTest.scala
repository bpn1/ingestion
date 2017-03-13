import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json.Json

import scala.io.Source
import scala.util.matching.Regex

class WikiDataRDDTest extends FlatSpec with SharedSparkContext with Matchers {
	// def cleanJSON(json: String): String

	"json cleaning" should "return valid json" in {
		// invalid json would cause an exception
		wikiDataTestData
			.map(WikiDataRDD.cleanJSON)
		    .filter(_.nonEmpty)
			.map(Json.parse)
	}

	// TODO json test dataset
	// def getValue(json: JsValue, path: List[String]): Option[JsValue]

	// def extractString(json: JsValue, path: List[String]): Option[String]

	// def extractList(json: JsValue, path: List[String]): List[JsValue]

	// def extractMap(json: JsValue, path: List[String]): Map[String, JsValue]

	// def extractDouble(json: JsValue, path: List[String]): Option[String]


	// def parseDataType(dataType: Option[String], dataValue: JsValue): String

	// def fillEntityValues(json: JsValue): WikiDataEntity

	// def extractClaimValues(claim: JsValue): String

	// def parseEntity(line: String): WikiDataEntity

	// def translatePropertyIDs(entity: WikiDataEntity, properties: Map[String, String]):
	// WikiDataEntity

	// def buildPropertyMap(rdd: RDD[WikiDataEntity]): Map[String, String]

//	they should "have valid IDs" in {
//		val validIDs = getRDD()
//			.filter { entity =>
//				val idRegex = new Regex("^(P|Q)[0-9]+$")
//				idRegex.findFirstIn(entity.id) != None
//			}
//
//		validIDs.count should be (getRDD().count)
//	}
//
//	they should "have a label" in {
//		val labelFound = getRDD()
//			.filter { entity =>
//				entity.label != "" && entity.label != null
//			}
//
//		labelFound.count should be (getRDD().count)
//	}
//
//	they should "be of type item or property" in {
//		val invalidTypes = getRDD()
//			.filter { entity =>
//				entity.entitytype == "item" || entity.entitytype == "property"
//			}
//
//		invalidTypes.count should be (getRDD().count)
//	}
//
//	"WikiData Property map" should "not be empty" in {
//		getPropertyMap() should not be empty
//	}
//
//	it should "contain only property IDs" in {
//		var properties = getPropertyMap()
//		val pidRegex = new Regex("^P[0-9]+$")
//		var validIDs = properties.filter { case (id, label) =>
//			pidRegex.findFirstIn(id) != None
//		}
//
//		validIDs.size should be (properties.size)
//	}
//
//	it should "have a label for each ID" in {
//		var properties = getPropertyMap()
//		var validLabels = properties.filter { case (id, label) =>
//			label != "" && label != null
//		}
//
//		validLabels.size should be (properties.size)
//	}
//
//	"Translated WikiData entries" should "not contain any property IDs" in {
//		val pidRegex = new Regex("^P[0-9]+$")
//		var translated = WikiDataRDD
//			.translateProps(getRDD(), sc.broadcast(getPropertyMap()))
//			.filter(entity => {
//				val translatedProps = entity.data.filter { case (id, value) =>
//					pidRegex.findFirstIn(id) != None
//				}
//
//				entity.data.size == translatedProps.size
//			})
//
//		translated.count should be (getRDD().count)
//	}

	def getPropertyMap(): Map[String, String] = {
		// TODO create test dataset
		WikiDataRDD.buildPropertyMap(
			sc.parallelize(wikiDataTestData())
				.map(WikiDataRDD.cleanJSON)
				.filter(_.nonEmpty)
				.map(WikiDataRDD.parseEntity))
	}

	// extracted from WikiData dump
	def wikiDataTestData(): List[String] = {
		Source.fromURL(getClass.getResource("/wikidataTestData"))
			.getLines()
			.toList
	}
}
