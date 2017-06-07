package de.hpi.ingestion.dataimport.dbpedia

import java.io.ByteArrayOutputStream

import org.scalatest.{FlatSpec, Matchers}

class DBpediaDumpURLsTest extends FlatSpec with Matchers {
	"Command line arguments" should "be asserted" in {
		val output = new ByteArrayOutputStream()
		Console.withOut(output) {
			DBpediaDumpURLs.main(Array[String]())
		}
		output.toString should not be empty
	}

	"Urls" should "be printed" in {
		val output = new ByteArrayOutputStream()
		Console.withOut(output) {
			DBpediaDumpURLs.main(Array("src/test/resources/dbpedia.html"))
		}
		output.toString should not be empty
	}
}
