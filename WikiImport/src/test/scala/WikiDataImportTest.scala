import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext
import play.api.libs.json.{JsObject, Json}
import scala.io.Source

class WikiDataImportTest extends FlatSpec with SharedSparkContext with Matchers {
	val datatypeFile = "claim_datatypes.json"

	"Json" should "be cleaned and valid json" in {
		// invalid json would cause an exception
		TestData.rawWikiDataEntries()
			.map(WikiDataImport.cleanJSON)
		    .filter(_.nonEmpty)
			.map(Json.parse)
	}

	"Data types" should "be parsed" in {
		val jsonFile = Source.fromURL(getClass.getResource(s"$datatypeFile"))
		val claimJson = Json.parse(jsonFile.getLines().mkString("\n")).as[JsObject]
		claimJson.value.foreach { case (dataType, jsonData) =>
			val extractedData = WikiDataImport.parseDataType(Option(dataType), jsonData)
			val expectedValue = TestData.dataTypeValues(dataType)
			extractedData shouldBe expectedValue
		}
	}

	"Entity values" should "be filled in" in {
		// def fillEntityValues(json: JsValue): WikiDataEntity
		assert(false)
	}

	"Claim values" should "be extracted" in {
		val jsonFile = Source.fromURL(getClass.getResource(s"$datatypeFile"))
		val claimJson = Json.parse(jsonFile.getLines().mkString("\n")).as[JsObject]
		claimJson.value.foreach { case (dataType, jsonData) =>
			val extractedData = WikiDataImport.extractClaimValues(jsonData)
			val expectedValue = TestData.dataTypeValues(dataType)
			extractedData shouldBe expectedValue
		}
	}

	"Wikidata entity" should "be parsed" in {
		// def parseEntity(line: String): WikiDataEntity
		assert(false)
	}

	"Property ids" should "be translated" in {
		// def translatePropertyIDs(entity: WikiDataEntity, properties: Map[String, String]):
		assert(false)
	}

	"Property map" should "be built" in {
		// def buildPropertyMap(rdd: RDD[WikiDataEntity]): Map[String, String]
		assert(false)
	}

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
		WikiDataImport.buildPropertyMap(
			sc.parallelize(TestData.rawWikiDataEntries())
				.map(WikiDataImport.cleanJSON)
				.filter(_.nonEmpty)
				.map(WikiDataImport.parseEntity))
	}

}
