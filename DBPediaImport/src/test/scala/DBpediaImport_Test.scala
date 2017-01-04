import org.scalatest.FunSpec
import DBPediaImport._
/**
	* Created by Lando on 03.01.17.
	*/
abstract class DBpediaImport_Spec extends FunSpec {
	describe("DBpediaImport") {

		describe("when parsing turtle file") {
			val line = "<http://dbpedia.org/resource/Spiderman> <http://www.perceive.net/schemas/relationship/enemyOf> <http://dbpedia.org/resource/Green_Goblin> ."
			it("should extract the triple") {
				val triple = List("http://dbpedia.org/resource/Spiderman", "http://www.perceive.net/schemas/relationship/enemyOf", "http://dbpedia.org/resource/Green_Goblin")
				assert(tokenize(line) == triple)
			}

			it("should replace prefixes") {
				val triple = List("dbr:Spiderman", "http://www.perceive.net/schemas/relationship/enemyOf", "dbr:Green_Goblin")
				assert(tokenize(line).map(cleanURL) == triple)
			}

			it("should create a DBpediaTriple with subject/predicate/property") {
				val parsedTriple = parseLine(line)
				assert(parsedTriple.subject == "http://example.org/#spiderman")
				assert(parsedTriple.predicate == "http://www.perceive.net/schemas/relationship/enemyOf")
				assert(parsedTriple.property == "http://example.org/#green-goblin")
			}
		}
		describe("when creating DBpediaEntities") {
			val triples = List(
				DBPediaTriple("dbr:Spiderman", "http://www.perceive.net/schemas/relationship/enemyOf", "dbr:Green_Goblin"),
				DBPediaTriple("dbr:Spiderman", "http://www.perceive.net/schemas/relationship/enemyOf", "dbr:Red_Goblin"),
				DBPediaTriple("dbr:Spiderman", "rdfs:label", "Spiderman"),
				DBPediaTriple("dbr:American_Airlines", "dbo:wikiPageID", "2386")
			)
			it("should extract properties") {
				val group = Tuple2("dbr:Spiderman", List(DBPediaTriple("dbr:Spiderman", "http://www.perceive.net/schemas/relationship/enemyOf", "dbr:Green_Goblin"), DBPediaTriple("dbr:Spiderman", "http://www.perceive.net/schemas/relationship/enemyOf", "dbr:Red_Goblin"), DBPediaTriple("dbr:Spiderman", "rdfs:label", "Spiderman")))

			}
		}
	}
}
