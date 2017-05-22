package de.hpi.ingestion.textmining.models

/**
  * Represents the prototype of an entry used to train a classifier. Contains all precomputed values of the final
  * Feature Entry.
  * @param alias alias of the link
  * @param link Link his Entry represents
  * @param linkContext context of the link
  * @param totalProbability probability that this entries alias is a link
  * @param pageProbability probability that this entries alias links to the page
  */
case class ProtoFeatureEntry(
	alias: String,
	link: Link,
	linkContext: Map[String, Double],
	totalProbability: Double,
	pageProbability: Double
) {
	/**
	  * Transforms this entry into a Feature entry by filling in the missing values.
	  * @param page page this entry might link to
	  * @param cosineSim cosine similarity of this entries link context and the pages article context
	  * @return Feature Entry used to train a classifier
	  */
	def toFeatureEntry(page: String, cosineSim: Double): FeatureEntry = {
		FeatureEntry(alias, page, totalProbability, pageProbability, cosineSim, link.page == page)
	}
}
