package de.hpi.ingestion.deduplication

object DeduplicationJob {
	def main(args: Array[String]): Unit = {
		val deduplication = new Deduplication(0.5, "TestDeduplication", List("testSource"))

		val geoCountryBlockingScheme = new ListBlockingScheme
		geoCountryBlockingScheme.setAttributes("geo_city")
		geoCountryBlockingScheme.tag = "geo Country BlockingScheme"

		val generalSectorBlockingScheme = new ListBlockingScheme
		generalSectorBlockingScheme.setAttributes("gen_sectors")
		generalSectorBlockingScheme.tag = "general Sector BlockingScheme"

		val simpleBlockingScheme = new SimpleBlockingScheme

		deduplication.blockingSchemes = List(
			geoCountryBlockingScheme,
			generalSectorBlockingScheme,
			simpleBlockingScheme
		)
		deduplication.main(args)
	}
}
