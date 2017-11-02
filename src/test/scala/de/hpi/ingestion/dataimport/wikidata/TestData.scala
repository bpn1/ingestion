/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.dataimport.wikidata

import java.util.UUID
import scala.io.Source
import org.apache.spark.SparkContext
import de.hpi.ingestion.dataimport.wikidata.models.{SubclassEntry, WikidataEntity}
import de.hpi.ingestion.datalake.models.{Subject, Version}

// scalastyle:off number.of.methods
// scalastyle:off line.size.limit
object TestData {
    val wikidataEntriesPath = "/wikidata/wikidata_entries.json"
    val testEntriesPath = "/wikidata/test_entities.json"
    val claimDataPath = "/wikidata/claim_data.json"

    def unfilteredWikidataEntities(): List[WikidataEntity] = {
        List(WikidataEntity("Q5", label = Option("Mensch"), instancetype = Option("Don't resolve me")),
            WikidataEntity("Q11570", label = Option("Kilogramm")),
            WikidataEntity("Q9212", label = Option("United States Army")))
    }
    def filteredWikidataEntities(): List[WikidataEntity] = {
        List(WikidataEntity("Q11570", label = Option("Kilogramm")),
            WikidataEntity("Q9212", label = Option("United States Army")))
    }

    def unresolvedWikidataEntities(): List[WikidataEntity] = {
        List(WikidataEntity("Q433465", label = Option("Jill Bakken"), data = Map("mass" -> List("+65;Q11570"), "instance of" -> List("Q5"), "military branch" -> List("Q9212"))))
    }

    def entityNameData(): List[(String, String)] = {
        List(("Q5", "Mensch"),
            ("Q11570", "Kilogramm"),
            ("Q9212", "United States Army"))
    }

    def flattenedWikidataEntries(): List[(String, String, String)] = {
        List(("Q433465", "mass", "+65;Q11570"),
            ("Q433465", "instance of", "Q5"),
            ("Q433465", "military branch", "Q9212"))
    }

    def wikidataIdEntries(): List[(String, String, String)] = {
        List(("Q433465", "instance of", "Q5"),
            ("Q433465", "military branch", "Q9212"))
    }

    def resolvedWikidataIdEntries(): List[(String, String, String)] = {
        List(("Q433465", "instance of", "Mensch"),
            ("Q433465", "military branch", "United States Army"))
    }

    def unitWikidataIdEntries(): List[(String, String, String)] = {
        List(("Q433465", "mass", "+65;Q11570"))
    }

    def splitUnitWikidataIdEntries(): List[(String, (String, String, String))] = {
        List(("Q11570", ("Q433465", "mass", "+65")))
    }

    def resolvedUnitWikidataIdEntries(): List[(String, String, String)] = {
        List(("Q433465", "mass", "+65;Kilogramm"))
    }

    def wikidataIdKey: String = {
        val job = new FindRelations
        job.settings("wikidataIdKey")
    }

    def unresolvedSubjects(): List[Subject] = {
        List(Subject(id = UUID.fromString("41e7b945-0f73-430a-be7c-580fc7a09f58"), master = null, datasource = "wikidata", name = Option("Entry 1"), properties = Map(wikidataIdKey -> List("Q1"), "test" -> List("Q2")), relations = Map(UUID.fromString("7f6891ef-c72a-4af5-a4af-f134f846413f") -> Map("key 1" -> "value 1"))),
            Subject(id = UUID.fromString("7f6891ef-c72a-4af5-a4af-f134f846413f"), master = null, datasource = "wikidata", name = Option("Entry 2"), properties = Map(wikidataIdKey -> List("Q2"), "test" -> List("Q3"))),
            Subject(id = UUID.fromString("5788c6f5-9696-4d67-a592-fd9b8c4e5a9d"), master = null, datasource = "wikidata", name = Option("Entry 3"), properties = Map(wikidataIdKey -> List("Q3"), "test" -> List("Q4"))),
            Subject(id = UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d"), master = null, datasource = "wikidata", name = Option("Entry 4"), properties = Map(wikidataIdKey -> List("Q4"))),
            Subject(id = UUID.fromString("7bfd2ffe-154b-486a-b30d-581d785940c6"), master = null, datasource = "wikidata", name = Option("Entry 5"), properties = Map(wikidataIdKey -> Nil, "test" -> List("Q1"))),
            Subject(id = UUID.fromString("bbd8f942-1663-4fea-9e70-3cf27896bc57"), master = null, datasource = "wikidata", name = Option("Entry 6"), properties = Map("test" -> List("Q5"))))
    }

    def resolvedNameMap(): Map[String, (UUID, String)] = {
        Map("Q1" -> (UUID.fromString("41e7b945-0f73-430a-be7c-580fc7a09f58"), "Entry 1"),
            "Q2" -> (UUID.fromString("7f6891ef-c72a-4af5-a4af-f134f846413f"), "Entry 2"),
            "Q3" -> (UUID.fromString("5788c6f5-9696-4d67-a592-fd9b8c4e5a9d"), "Entry 3"),
            "Q4" -> (UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d"), "Entry 4"))
    }

    def resolvedSubjects(): List[Subject] = {
        List(Subject(id = UUID.fromString("41e7b945-0f73-430a-be7c-580fc7a09f58"), master = null, datasource = "wikidata", name = Option("Entry 1"), properties = Map(wikidataIdKey -> List("Q1"), "test" -> List("Entry 2")), relations = Map(UUID.fromString("7f6891ef-c72a-4af5-a4af-f134f846413f") -> Map("test" -> "", "key 1" -> "value 1"))),
            Subject(id = UUID.fromString("7f6891ef-c72a-4af5-a4af-f134f846413f"), master = null, datasource = "wikidata", name = Option("Entry 2"), properties = Map(wikidataIdKey -> List("Q2"), "test" -> List("Entry 3")), relations = Map(UUID.fromString("5788c6f5-9696-4d67-a592-fd9b8c4e5a9d") -> Map("test" -> ""))),
            Subject(id = UUID.fromString("5788c6f5-9696-4d67-a592-fd9b8c4e5a9d"), master = null, datasource = "wikidata", name = Option("Entry 3"), properties = Map(wikidataIdKey -> List("Q3"), "test" -> List("Entry 4")), relations = Map(UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d") -> Map("test" -> ""))),
            Subject(id = UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d"), master = null, datasource = "wikidata", name = Option("Entry 4"), properties = Map(wikidataIdKey -> List("Q4"))),
            Subject(id = UUID.fromString("7bfd2ffe-154b-486a-b30d-581d785940c6"), master = null, datasource = "wikidata", name = Option("Entry 5"), properties = Map("test" -> List("Entry 1")), relations = Map(UUID.fromString("41e7b945-0f73-430a-be7c-580fc7a09f58") -> Map("test" -> ""))),
            Subject(id = UUID.fromString("bbd8f942-1663-4fea-9e70-3cf27896bc57"), master = null, datasource = "wikidata", name = Option("Entry 6"), properties = Map("test" -> List("Q5"))))
    }

    def completeWikidataEntities(): List[WikidataEntity] = {
        List(WikidataEntity("Q21110253", List("testalias"), Option("human protein (annotated by UniProtKB/Swiss-Prot Q8N128)"), Option("item"), Option("testwikiname"), Option("en_testwikiname"), Option("test_instancetype"), Option("Protein FAM177A1"), Map("Ensembl Protein ID" -> List("ENSP00000280987", "ENSP00000371843", "ENSP00000379734"), "subclass of" -> List("Protein", "FAM177 family"))))
    }

    def translatedSubjects(): List[Subject] = {
        List(
            Subject(
                master = null,
                datasource = "wikidata",
                name = Option("Protein FAM177A1"),
                aliases = List("testalias"),
                category = Option("test_instancetype"),
                properties = Map(
                    "wikipedia_name" -> List("testwikiname"),
                    "Ensembl Protein ID" -> List("ENSP00000280987", "ENSP00000371843", "ENSP00000379734"),
                    "subclass of" -> List("Protein", "FAM177 family"),
                    "wikidata_id" -> List("Q21110253")
                )
            )
        )
    }

    def classWikidataEntities(): List[WikidataEntity] = {
        List(
            WikidataEntity("Q1", label = Option("Entry 1"), data = Map("instance of" -> List("Q2"), "subclass of" -> List("Q3"), "test" -> List("testVal"))),
            WikidataEntity("Q2", label = Option("Entry 2"), data = Map("instance of" -> List("Q1"), "test" -> List("testVal"))),
            WikidataEntity("Q3", label = Option("Entry 3"), data = Map("subclass of" -> List("Q4"))),
            WikidataEntity("Q4", label = Option("Entry 4"), data = Map("test" -> List("testVal"))))
    }

    def subclassEntries(): List[SubclassEntry] = {
        List(
            SubclassEntry("Q1", "Entry 1", Map("instance of" -> List("Q2"), "subclass of" -> List("Q3"))),
            SubclassEntry("Q2", "Entry 2", Map("instance of" -> List("Q1"))),
            SubclassEntry("Q3", "Entry 3", Map("subclass of" -> List("Q4"))),
            SubclassEntry("Q4", "Entry 4"))
    }

    def subclassOfProperties(): List[SubclassEntry] = {
        List(
            SubclassEntry("Q1", "Entry 1", Map("instance of" -> List("Q2"), "subclass of" -> List("Q3"))),
            SubclassEntry("Q3", "Entry 3", Map("subclass of" -> List("Q4"))))
    }

    def oldClassMap(): Map[String, List[String]] = {
        Map(
            "Q3" -> List("Entry 4", "Entry 5", "Entry 3"),
            "Q4" -> List("Entry 4"))
    }

    def newClassMap(): Map[String, List[String]] = {
        Map(
            "Q1" -> List("Entry 4", "Entry 3", "Entry 1"),
            "Q3" -> List("Entry 4", "Entry 3"),
            "Q4" -> List("Entry 4", "Entry 4", "Entry 3"))
    }

    def classMap(): Map[String, List[String]] = {
        Map(
            "Q1" -> List("Entry 4", "Entry 3", "Entry 1"),
            "Q3" -> List("Entry 4", "Entry 3"),
            "Q4" -> List("Entry 4"))
    }

    def reducableClassMaps(): List[Map[String, List[String]]] = {
        List(
            Map(
                "Q1" -> List("Entry 4", "Entry 3", "Entry 1"),
                "Q4" -> List("Entry 4", "Entry 3")
            ),
            Map(
                "Q1" -> List("Entry 4", "Entry 3", "Entry 1", "Entry 0"),
                "Q3" -> List("Entry 4", "Entry 3", "Entry 1"),
                "Q4" -> List("Entry 4")
            ),
            Map(
                "Q3" -> List("Entry 4", "Entry 3"),
                "Q4" -> List("Entry 4", "Entry 1")
            )
        )
    }

    def validInstanceOfProperties(): List[SubclassEntry] = {
        List(SubclassEntry("Q2", "Entry 2", Map("instance of" -> List("Q1"))))
    }

    def updatedInstanceOfProperties(): List[(String, String, Map[String, List[String]])] = {
        List(("Q2", "Entry 4", Map("wikidata_path" -> List("Entry 4", "Entry 3", "Entry 1"))))
    }

    def classesToTag(): Map[String, String] = {
        Map("Q4" -> "Entry 4")
    }

    def rawWikidataEntries(): List[String] = {
        Source.fromURL(getClass.getResource(wikidataEntriesPath))
            .getLines()
            .toList
    }

    def claimData(): String = {
        Source.fromURL(getClass.getResource(claimDataPath))
            .getLines()
            .mkString("\n")
    }

    def dataTypeValues(dataType: String): Option[String] = {
        dataType match {
            case "string" => Option("test string 1")
            case "wikibase-entityid" => Option("test id 1")
            case "time" => Option("test time 1")
            case "monolingualtext" => Option("test text 1")
            case "globecoordinate" => Option("1.0;2.0")
            case "quantity" => Option("test amount;test unit")
            case "" => None
        }
    }

    def propertyEntities(): List[WikidataEntity] = {
        List(
            WikidataEntity("P1", label = Option("Property 1"), entitytype = Option("property")),
            WikidataEntity("P2", label = Option("Property 2"), entitytype = Option("property")),
            WikidataEntity("P3", label = Option("Property 3"), entitytype = Option("property")),
            WikidataEntity("P4", label = Option("Property 4"), entitytype = Option("property")),
            WikidataEntity("P5", label = Option("Property 5")),
            WikidataEntity("P6", entitytype = Option("property")))
    }

    def propertyMap(): Map[String, String] = {
        Map("P1" -> "Property 1", "P2" -> "Property 2", "P3" -> "Property 3", "P4" -> "Property 4")
    }

    def rawTestEntries(): String = {
        Source.fromURL(getClass.getResource(testEntriesPath))
            .getLines()
            .mkString("\n")
    }

    def filledWikidataEntities(): List[WikidataEntity] = {
        List(
            WikidataEntity("ID 1", entitytype = Option("property"), description = Option("de description value"), label = Option("de label"), wikiname = Option("de wiki title")),
            WikidataEntity("ID 2", entitytype = Option("property"), description = Option("de description value"), label = Option("en label"), wikiname = Option("de wiki title")),
            WikidataEntity("ID 3", entitytype = Option("property"), description = Option("en description value"), label = Option("en label")),
            WikidataEntity("ID 4", entitytype = Option("property"), label = Option("bla label")),
            WikidataEntity("ID 5", entitytype = Option("entity"), description = Option("de description value"), label = Option("de label"), wikiname = Option("de wiki title")),
            WikidataEntity("ID 6", entitytype = Option("entity"), description = Option("de description value"), label = Option("de label"), wikiname = Option("de wiki title")),
            WikidataEntity("ID 7", entitytype = Option("entity"), description = Option("en description value"), label = Option("en label")),
            WikidataEntity("ID 8", entitytype = Option("entity2"), label = Option("bla label")),
            WikidataEntity("ID 9"))
    }

    def parsedWikidataEntities(): List[WikidataEntity] = {
        List(
            WikidataEntity("ID 1", aliases = List("de alias", "en alias"),entitytype = Option("property"), description = Option("de description value"), label = Option("de label"), wikiname = Option("de wiki title"), data = Map("P1" -> List("test string 1", "test time 1"))),
            WikidataEntity("ID 2", aliases = List("de alias"), entitytype = Option("property"), description = Option("de description value"), label = Option("en label"), wikiname = Option("de wiki title")),
            WikidataEntity("ID 3", aliases = List("en alias"), entitytype = Option("property"), description = Option("en description value"), label = Option("en label"), data = Map("P2" -> List("test text 1"))),
            WikidataEntity("ID 4", entitytype = Option("property"), label = Option("bla label")),
            WikidataEntity("ID 5", entitytype = Option("entity"), description = Option("de description value"), label = Option("de label"), wikiname = Option("de wiki title")),
            WikidataEntity("ID 6", aliases = List("asd alias"), entitytype = Option("entity"), description = Option("de description value"), label = Option("de label"), wikiname = Option("de wiki title")),
            WikidataEntity("ID 7", entitytype = Option("entity"), description = Option("en description value"), label = Option("en label")),
            WikidataEntity("ID 8", entitytype = Option("entity2"), label = Option("bla label")),
            WikidataEntity("ID 9", data = Map("P3" -> List("test amount;test unit"), "P4" -> List("1.0;2.0"))))
    }

    def translatedWikidataEntities(): List[WikidataEntity] = {
        List(
            WikidataEntity("ID 1", aliases = List("de alias", "en alias"),entitytype = Option("property"), description = Option("de description value"), label = Option("de label"), wikiname = Option("de wiki title"), data = Map("Property 1" -> List("test string 1", "test time 1"))),
            WikidataEntity("ID 2", aliases = List("de alias"), entitytype = Option("property"), description = Option("de description value"), label = Option("en label"), wikiname = Option("de wiki title")),
            WikidataEntity("ID 3", aliases = List("en alias"), entitytype = Option("property"), description = Option("en description value"), label = Option("en label"), data = Map("Property 2" -> List("test text 1"))),
            WikidataEntity("ID 4", entitytype = Option("property"), label = Option("bla label")),
            WikidataEntity("ID 5", entitytype = Option("entity"), description = Option("de description value"), label = Option("de label"), wikiname = Option("de wiki title")),
            WikidataEntity("ID 6", aliases = List("asd alias"), entitytype = Option("entity"), description = Option("de description value"), label = Option("de label"), wikiname = Option("de wiki title")),
            WikidataEntity("ID 7", entitytype = Option("entity"), description = Option("en description value"), label = Option("en label")),
            WikidataEntity("ID 8", entitytype = Option("entity2"), label = Option("bla label")),
            WikidataEntity("ID 9", data = Map("Property 3" -> List("test amount;test unit"), "Property 4" -> List("1.0;2.0"))))
    }

    def entityLabels(): List[Option[String]] = {
        List(
            Option("de label"),
            Option("en label"),
            Option("en label"),
            Option("bla label"),
            Option("de label"),
            Option("de label"),
            Option("en label"),
            Option("bla label"),
            None)
    }

    def entityAliases(): List[List[String]] = {
        List(
            List("de alias", "en alias"),
            List("de alias"),
            List("en alias"),
            List(),
            List(),
            List("asd alias"),
            List(),
            List(),
            List())
    }

    def unfilteredEntities: List[WikidataEntity] = List(
        WikidataEntity(id = "Q1", instancetype = Option("type 1")),
        WikidataEntity(id = "Q2", instancetype = Option("type 2")),
        WikidataEntity(id = "Q3", instancetype = Option("type 3")),
        WikidataEntity(id = "Q4", instancetype = Option(null)),
        WikidataEntity(id = "Q5")
    )

    def filteredEntities: List[WikidataEntity] = List(
        WikidataEntity(id = "Q1", instancetype = Option("type 1")),
        WikidataEntity(id = "Q2", instancetype = Option("type 2")),
        WikidataEntity(id = "Q3", instancetype = Option("type 3"))
    )

    def version(sc: SparkContext): Version = Version("DBpediaDataLakeImport", List("dataSources"), sc, false, None)

    def testEntity: WikidataEntity = {
        WikidataEntity(
            "Q21110253",
            List("testalias"),
            Option("human protein (annotated by UniProtKB/Swiss-Prot Q8N128)"),
            Option("item"),
            Option("testwikiname"),
            Option("en_testwikiname"),
            Option("test_instancetype"),
            Option("Protein"),
            Map(
                "Ensembl Protein ID" -> List("ENSP00000280987", "ENSP00000371843", "ENSP00000379734"),
                "subclass of" -> List("Protein", "FAM177 family"),
                "VIAF ID" -> List("X123"),
                "industry" -> this.unnormalizedSectors,
                "coordinate location" -> this.unnormalizedCoords,
                "headquarters location" -> this.unnormalizedCities,
                "country" -> this.unnormalizedCountries,
                "employees" -> this.unnormalizedEmployees,
                "official website" -> this.unnormalizedURLs,
                "legal form" -> List("Aktiengesellschaft"),
                "testProperty" -> List("test")
            )
        )
    }

    def wikidataEntities: List[WikidataEntity] = List(
        WikidataEntity(
            "Q21110253",
            List("testalias"),
            Option("human protein (annotated by UniProtKB/Swiss-Prot Q8N128)"),
            Option("item"),
            Option("testwikiname"),
            Option("en_testwikiname"),
            Option("company"),
            Option("Protein FAM177A1"),
            Map(
                "Ensembl Protein ID" -> List("ENSP00000280987", "ENSP00000371843", "ENSP00000379734"),
                "subclass of" -> List("Protein", "FAM177 family"),
                "VIAF ID" -> List("X123"),
                "testProperty" -> List("test"))
        ),
        WikidataEntity("Q123")
    )

    def mapping: Map[String, List[String]] = Map(
        "id_wikidata" -> List("id"),
        "id_dbpedia" -> List("wikiname"),
        "id_wikipedia" -> List("wikiname"),
        "id_viaf" -> List("VIAF ID"),
        "gen_sectors" -> List("industry"),
        "geo_coords" -> List("coordinate location"),
        "geo_city" -> List("headquarters location"),
        "geo_country" -> List("country"),
        "gen_employees" -> List("employees"),
        "gen_urls" -> List("official website"),
        "gen_legal_form" -> List("legal form")
    )

    def strategies: Map[String, List[String]] = Map(
        "Automobilindustrie" -> List("29", "45"),
        "Einzelhandel" -> List("47")
    )

    def unnormalizedSectors: List[String] = List("Automobilindustrie", "Q126793", "Einzelhandel")
    def normalizedSectors: List[String] = List("Automobilindustrie", "Einzelhandel")
    def mappedSectors: List[String] = List("29", "45", "47")
    def unnormalizedCoords: List[String] = List("-1;1", "55;48.88", "0.133;-1", "xxx;-1.0")
    def normalizedCoords: List[String] = List("-1;1", "55;48.88", "0.133;-1")
    def unnormalizedCountries: List[String] = List("Q159", "Q631750", "Russland", "Igrinski rajon")
    def normalizedCountries: List[String] = List("RU")
    def unnormalizedCities: List[String] = List("Q159", "Q631750", "Russland", "Igrinski rajon")
    def normalizedCities: List[String] = List("Russland", "Igrinski rajon")
    def unnormalizedEmployees: List[String] = List("+500;1", "+1337;1", "WRONG")
    def normalizedEmployees: List[String] = List("500", "1337")
    def unnormalizedURLs: List[String] = List("https://youtube.de", "http://facebook.de", "http://www.google.de", "www.hans", "NotAURL")
    def normalizedURLs: List[String] = List("https://youtube.de", "http://facebook.de", "http://www.google.de")

    def applyInput: List[List[String]] = List(
        this.unnormalizedCoords,
        this.unnormalizedCities,
        this.unnormalizedCountries,
        this.unnormalizedSectors,
        this.unnormalizedEmployees,
        this.unnormalizedURLs,
        List("default")
    )
    def applyAttributes: List[String] = List("geo_coords", "geo_city", "geo_country", "gen_sectors", "gen_employees", "gen_urls", "default")
    def applyStrategies: List[(List[String] => List[String])] = List(
        WikidataNormalizationStrategy.normalizeCoords,
        WikidataNormalizationStrategy.normalizeCity,
        WikidataNormalizationStrategy.normalizeCountry,
        WikidataNormalizationStrategy.normalizeSector,
        WikidataNormalizationStrategy.normalizeEmployees,
        WikidataNormalizationStrategy.normalizeURLs,
        identity
    )
    def unnormalizedAttributes: Map[String, List[String]] = Map(
        "gen_sectors" -> (this.unnormalizedSectors :+ "Test_Sector"),
        "geo_coords" -> this.unnormalizedCoords,
        "geo_country" -> this.unnormalizedCountries,
        "geo_city" -> this.unnormalizedCities,
        "gen_employees" -> this.unnormalizedEmployees,
        "gen_urls" -> this.unnormalizedURLs
    )
    def normalizedAttributes: Map[String, List[String]] = Map(
        "gen_sectors" -> (this.mappedSectors :+ "Test_Sector"),
        "geo_coords" -> this.normalizedCoords,
        "geo_country" -> this.normalizedCountries,
        "geo_city" -> this.normalizedCities,
        "gen_employees" -> this.normalizedEmployees,
        "gen_urls" -> this.normalizedURLs
    )
}
// scalastyle:on line.size.limit
// scalastyle:on number.of.methods
