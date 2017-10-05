package de.hpi.ingestion.framework

import org.rogach.scallop.ScallopConf

class CommandLineConf(arguments: Seq[String]) extends ScallopConf(arguments) {
	val config = opt[String](descr = "config file for accessing tables and the deduplication score configs")
	val importConfig = opt[String](descr = "config file for the normalization")
	val commitJson = opt[String](descr = "JSON string used for the commit job by the Curation Interface")
	val comment = opt[String](descr = "comment used by jobs that write one (e.g. Blocking)")
	val tokenizer = opt[String](descr = "tokenizer used for the TermFrequencyCounter")
	val toReduced = opt[Boolean](descr = "if set the Link Analysis will write to the reduced column")
	val restoreVersion = opt[String](descr = "Version to which the Subjects are restored")
	val diffVersions = opt[List[String]](descr = "Versions used in the VersionDiff job")
	verify()
}
