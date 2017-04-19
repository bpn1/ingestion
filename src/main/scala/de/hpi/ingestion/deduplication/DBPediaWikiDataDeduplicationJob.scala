package de.hpi.ingestion.deduplication

object DBPediaWikiDataDeduplicationJob {
	def main(args: Array[String]): Unit = {
		val deduplication = new Deduplication(
			0.5,
			"DBPediaWikiDataDeduplication",
			List("DBPedia", "WikiData"),
			Option("config_dbpedia_wikidata.xml")
		)
		val geoCountryBlockingScheme = new ListBlockingScheme
		geoCountryBlockingScheme.setAttributes("geo_country")
		val generalSectoryBlockingScheme = new ListBlockingScheme
		generalSectoryBlockingScheme.setAttributes("gen_sectors")
		val simpleBlockingScheme = new SimpleBlockingScheme
		val blockingSchemes = List(
			geoCountryBlockingScheme,
			generalSectoryBlockingScheme,
			simpleBlockingScheme)
		deduplication.run(blockingSchemes)
	}
}
