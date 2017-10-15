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

case class CommandLineConf(
	configOpt: Option[String],
	importConfigOpt: Option[String],
	commitJsonOpt: Option[String],
	commentOpt: Option[String],
	tokenizerOpt: Option[List[String]],
	toReduced: Boolean,
	restoreVersionOpt: Option[String],
	diffVersionsOpt: Option[List[String]]
) {
	def config: String = configOpt.get
	def importConfig: String = importConfigOpt.get
	def commitJson: String = commitJsonOpt.get
	def comment: String = commentOpt.get
	def tokenizer: List[String] = tokenizerOpt.get
	def restoreVersion: String = restoreVersionOpt.get
	def diffVersions: List[String] = diffVersionsOpt.get
}
