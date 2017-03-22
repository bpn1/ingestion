import DataLake.Deduplication

object DeduplicationJob {
	def main(args : Array[String]): Unit = {
		val deduplication = new Deduplication(0.5, "TestDeduplication", List("testSource"))
		deduplication.run()
	}
}
