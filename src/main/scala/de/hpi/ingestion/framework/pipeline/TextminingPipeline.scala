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

package de.hpi.ingestion.framework.pipeline

import de.hpi.ingestion.textmining.ClassifierTraining
import de.hpi.ingestion.textmining.preprocessing._

class TextminingPipeline extends JobPipeline {
	pipelineName = "Textmining Pipeline"
	jobs = List(
		(new TextParser, Array.empty[String]),
		(new LinkCleaner, Array.empty[String]),
		(new RedirectResolver, Array.empty[String]),
		(new LinkAnalysis, Array.empty[String]),
		(new CompanyLinkFilter, Array.empty[String]),
		(new ReducedLinkAnalysis, Array.empty[String]),
		(new AliasTrieSearch, Array.empty[String]),
		(new LinkExtender, Array.empty[String]),
		(new TermFrequencyCounter, Array.empty[String]),
		(new DocumentFrequencyCounter, Array.empty[String]),
		(new LinkAnalysis, Array.empty[String]),
		(new ReducedLinkAnalysis, Array.empty[String]),
		(new AliasCounter, Array.empty[String]),
		(new CosineContextComparator, Array.empty[String]),
		(new SecondOrderFeatureGenerator, Array.empty[String]),
		(new ClassifierTraining, Array.empty[String]))
}
