package de.hpi.ingestion.deduplication

object DeduplicationJob {
	def main(args: Array[String]): Unit = {
		val deduplication = new Deduplication(0.5, "TestDeduplication", List("testSource"))
		deduplication.run()
	}
}
