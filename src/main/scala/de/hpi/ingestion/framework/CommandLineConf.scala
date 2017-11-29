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

/**
  * A serializable version of the CommandLineScallopConf.
  * @param configOpt Option of the config file
  * @param importConfigOpt Option of the import config file
  * @param commitJsonOpt Option of the Curation Commit JSON
  * @param commentOpt Option of the Blocking comment
  * @param tokenizerOpt Option of the Tokenizer flags used for the Term Frequency Counter
  * @param toReduced reduced flag used by the Link Analysis and Reduced Link Analysis
  * @param restoreVersionOpt Option of the Version restored to in the Version Restore
  * @param diffVersionsOpt Option of the Versions to diff in the Version Diff
  * @param sentenceEmbeddingFiles the Sentence Embedding files
  */
case class CommandLineConf(
    configOpt: Option[String],
    importConfigOpt: Option[String],
    commitJsonOpt: Option[String],
    commentOpt: Option[String],
    tokenizerOpt: Option[List[String]],
    toReduced: Boolean,
    restoreVersionOpt: Option[String],
    diffVersionsOpt: Option[List[String]],
    sentenceEmbeddingFiles: Map[String, String]
) {
    /**
      * Returns the value of configOpt and causes an error if it's not set.
      * @return value of configOpt
      */
    def config: String = configOpt.get

    /**
      * Returns the value of importConfigOpt and causes an error if it's not set.
      * @return value of importConfigOpt
      */
    def importConfig: String = importConfigOpt.get

    /**
      * Returns the value of commitJsonOpt and causes an error if it's not set.
      * @return value of commitJsonOpt
      */
    def commitJson: String = commitJsonOpt.get

    /**
      * Returns the value of commentOpt and causes an error if it's not set.
      * @return value of commentOpt
      */
    def comment: String = commentOpt.get

    /**
      * Returns the value of tokenizerOpt and causes an error if it's not set.
      * @return value of tokenizerOpt
      */
    def tokenizer: List[String] = tokenizerOpt.get

    /**
      * Returns the value of restoreVersionOpt and causes an error if it's not set.
      * @return value of restoreVersionOpt
      */
    def restoreVersion: String = restoreVersionOpt.get

    /**
      * Returns the value of diffVersionsOpt and causes an error if it's not set.
      * @return value of diffVersionsOpt
      */
    def diffVersions: List[String] = diffVersionsOpt.get
}

/**
  * Companion Object of the Command Line Conf.
  */
object CommandLineConf {
    /**
      * Constructor using the input command line arguments to create a Scallop Conf and transforms it into a
      * Command Line Conf.
      * @param arguments command line arguments
      * @return Command Line Conf containing the options set by the command line arguments
      */
    def apply(arguments: Seq[String]): CommandLineConf = {
        new CommandLineScallopConf(arguments).toCommandLineConf
    }
}
