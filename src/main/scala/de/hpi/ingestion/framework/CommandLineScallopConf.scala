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

import java.nio.charset.StandardCharsets

import com.google.common.io.BaseEncoding
import org.rogach.scallop.singleArgConverter
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.Help

import scala.util.matching.Regex

/**
  * Parses command line arguments into typed options used in Spark Jobs.
  * @param arguments command line arguments containing the configuration to be parsed
  */
class CommandLineScallopConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    banner("""Usage: spark.sh ... myJar.jar [OPTION]...
             |Options:
             |""".stripMargin)
    footer("\nFor more information visit the documentation in the GitHub Wiki:\n" +
        "https://github.com/bpn1/ingestion/wiki/Pass-Command-Line-Arguments")
    val config = opt[String](descr = "config file for accessing tables and the deduplication score configs")
    val importConfig = opt[String](descr = "config file for the normalization")
    val commitJson = opt[String](
        short = 'j',
        descr = "JSON string used for the commit job by the Curation Interface"
    )(singleArgConverter[String](commitConverter))
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
    val sentenceEmdbeddingDescription = "Specifies the files and parameters used for the sentence embedding " +
        "calculation. Possible keys are the following: stopwords, wordfrequencies, wordembeddings, weightparam. " +
        "The file paths should point to a file in the HDFS in the home folder of the HDFS " +
        "user that executes the Spark Job. The weightparam should be a double (e.g., 0.000001, 1e-7)."
    val sentenceEmbeddingFiles = props[String]('F', descr = sentenceEmdbeddingDescription)
    verify()

    /**
      * Called when an error parsing the arguments occurs. This can only happen when the tokenizer and the diff versions
      * are validated or the args contain wrong flags. If the args contain --help the help is printed. Otherwise an
      * IllegalArgumentException is thrown.
      * @param e Exception thrown by scallop
      * @throws java.lang.IllegalArgumentException contains the message of the input exception
      */
    @throws(classOf[IllegalArgumentException])
    override def onError(e: Throwable): Unit = {
        e match {
            case Help("") =>
                printHelp()
                throw new IllegalArgumentException(e.getMessage)
            case _ => throw new IllegalArgumentException(e.getMessage)
        }
    }

    /**
      * Transforms this Scallop Conf to a serializable Command Line Conf.
      * @return a Command Line Conf containing the same data of this Scallop Conf
      */
    def toCommandLineConf: CommandLineConf = {
        CommandLineConf(
            config.toOption,
            importConfig.toOption,
            commitJson.toOption,
            comment.toOption,
            tokenizer.toOption,
            toReduced(),
            restoreVersion.toOption,
            diffVersions.toOption,
            sentenceEmbeddingFiles.toList.toMap) // transforms non-serializable scallop map into a serializable map
    }

    /**
      * Checks if the commit string is base64 encoded and decodes it if it is. Otherwise the input is returned.
      * Regex from: https://stackoverflow.com/a/8571649
      * @param input the passed commit string
      * @return readable JSON commit string
      */
    def commitConverter(input: String): String = {
        val b64Regex = new Regex("^([A-Za-z0-9+\\/]{4})*([A-Za-z0-9+\\/]{4}|[A-Za-z0-9+\\/]{3}=|[A-Za-z0-9+\\/]{2}==)$")
        var commitString = input
        if (b64Regex.findFirstIn(input).isDefined) {
            val byteString = BaseEncoding.base64().decode(input)
            commitString = new String(byteString, StandardCharsets.UTF_8)
        }
        commitString
    }
}
