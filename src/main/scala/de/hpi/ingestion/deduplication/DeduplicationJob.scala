package de.hpi.ingestion.deduplication

object DeduplicationJob {
	def main(args: Array[String]): Unit = {
		val deduplication = new Deduplication(0.5, "TestDeduplication", List("testSource"))
		val geoCountryBlockingScheme = new ListBlockingScheme
		geoCountryBlockingScheme.setAttributes("geo_city")
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
