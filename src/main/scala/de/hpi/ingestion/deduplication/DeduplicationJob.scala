package de.hpi.ingestion.deduplication

object DeduplicationJob {
	def main(args: Array[String]): Unit = {
		val deduplication = new Deduplication(0.5, "TestDeduplication", List("testSource"))
		val blockingScheme1 = new ListBlockingScheme
		blockingScheme1.setAttributes("geo_country")
		val blockingScheme2 = new ListBlockingScheme
		blockingScheme2.setAttributes("gen_sectory")
		val blockingScheme3 = new SimpleBlockingScheme
		val blockingSchemes: List[BlockingScheme] = List(
			blockingScheme1,
			blockingScheme2,
			blockingScheme3)
		deduplication.run(blockingSchemes)
	}
}
