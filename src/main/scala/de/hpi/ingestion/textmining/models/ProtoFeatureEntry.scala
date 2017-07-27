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

package de.hpi.ingestion.textmining.models

/**
  * Represents the prototype of an entry used to train a classifier. Contains all precomputed values of the final
  * Feature Entry.
  *
  * @param link        Link this entry represents
  * @param linkContext context of the Link containing the tfidf values of the terms
  * @param linkScore   score for this entry's alias being a link
  * @param pageScore   score for this alias pointing to the specific page this object will be keyed by
  */
case class ProtoFeatureEntry(
	link: Link,
	linkContext: Map[String, Double],
	linkScore: Double,
	pageScore: MultiFeature
) {
	/**
	  * Transforms this entry into a Feature entry by filling in the missing values.
	  *
	  * @param page      page this entry might link to
	  * @param cosineSim cosine similarity of this entries link context and the pages article context
	  * @return Feature Entry used to train a classifier
	  */
	def toFeatureEntry(page: String, cosineSim: MultiFeature): FeatureEntry = {
		FeatureEntry(link.article.getOrElse("***Unknown article***"), link.offset.getOrElse(-1), link.alias, page,
			linkScore, pageScore, cosineSim, link.page == page)
	}
}
