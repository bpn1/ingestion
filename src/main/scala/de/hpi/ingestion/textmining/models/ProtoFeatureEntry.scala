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
