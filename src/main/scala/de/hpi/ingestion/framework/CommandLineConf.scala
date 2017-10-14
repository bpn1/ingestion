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
package de.hpi.ingestion.framework

import org.rogach.scallop.ScallopConf

class CommandLineConf(arguments: Seq[String]) extends ScallopConf(arguments) with Serializable {
	val config = opt[String](descr = "config file for accessing tables and the deduplication score configs")
	val importConfig = opt[String](descr = "config file for the normalization")
	val commitJson = opt[String](short = 'j', descr = "JSON string used for the commit job by the Curation Interface")
	val comment = opt[String](short = 'b', descr = "comment used by jobs that write one (e.g. Blocking)")
	val tokenizer = opt[List[String]](descr = "tokenizer used for the TermFrequencyCounter (up to three flags)")
	validate(tokenizer) {
		case flagList if flagList.length <= 3 => Right(Unit)
		case _ => Left("Only up to three tokenizer flags can be provided.")
	}
	val toReduced = opt[Boolean](short = 'r', descr = "if set the Link Analysis will write to the reduced column")
	val restoreVersion = opt[String](short = 'v', descr = "Version to which the Subjects are restored")
	val diffVersions = opt[List[String]](descr = "Versions used in the VersionDiff job (exactly 2)")
	validate(diffVersions) {
		case versionList if versionList.length == 2 => Right(Unit)
		case _ => Left("Exactly two versions have to be provided.")
	}
	verify()

	override def onError(e: Throwable): Unit = throw new IllegalArgumentException(e.getMessage)
}
