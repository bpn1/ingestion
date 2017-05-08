package de.hpi.ingestion.dataimport.dbpedia

import org.scalatest.{FlatSpec, Matchers}

class DBpediaNormalizeStrategyUnitTest extends FlatSpec with Matchers {
	def expectedNormalizeStrategies: List[List[String] => List[String]] = List(
		DBpediaNormalizeStrategy.normalizeEmployees,
		DBpediaNormalizeStrategy.normalizeCountry,
		DBpediaNormalizeStrategy.normalizeCoords,
		DBpediaNormalizeStrategy.normalizeNothing
	)

	"apply" should "decide, which strategy should be used regarding the input attribute" in {
		val input = List(
			"5500^^xsd:integer", "100^^xsd:nonNegativeInteger", "über 1000@de .", // employees
			"Deutschland@de .", "dbpedia-de:England", // country
			"48.34822^^xsd:float", "10.905282^^xsd:double", "48348220^^xsd:integer", "10905282^^xsd:integer", // coords
			"I don't fit anywhere :(" // nothing
		)
		val strategy = List(
			DBpediaNormalizeStrategy.apply("gen_employees"),
			DBpediaNormalizeStrategy.apply("geo_country"),
			DBpediaNormalizeStrategy.apply("geo_coords"),
			DBpediaNormalizeStrategy.apply("something_else")
		)
		val output = strategy.map(_(input))
		val expected = expectedNormalizeStrategies.map(_(input))
		output shouldEqual expected
	}

	//"normalizeCoords" should "normalize all possible appearances of coordinate values in dbpedia" in {}
	"normalizeCoords" should "kick out redundant information or integers" in {
		val input = List(
			"48.34822^^xsd:float", "10.905282^^xsd:float",
			"48348220^^xsd:integer", "10905282^^xsd:integer",
			"48.34822^^xsd:double", "10.905282^^xsd:double"
		)
		val output = DBpediaNormalizeStrategy("geo_coords")(input)
		val expected = List("48.34822", "10.905282")
		output shouldEqual expected
	}

	"normalizeCountry" should "normalize all possible appearances of country values in dbpedia" in {
		val input = List(
			"dbpedia-de:Japanisches_Kaiser-reich", "LI@de .", "Deutschland@de ."
		)
		val output = DBpediaNormalizeStrategy("geo_country")(input)
		val expected = List("Japanisches Kaiser-reich", "Deutschland")
		output shouldEqual expected
	}

	it should "remove integers and .svg files" in {
		val input = List(
			"dbpedia-de:Frankreich",
			"15^^xsd:integer",
			"dbpedia-de:Datei:Flag_of_Bavaria_(striped).svg"
		)
		val output = DBpediaNormalizeStrategy("geo_country")(input)
		val expected = List("Frankreich")
		output shouldEqual expected
	}
}
