package de.hpi.ingestion.textmining.models

/**
  * Case class representing an extended link between Wikipedia pages.
  *
  * @param alias  text this link appears as on the page
  * @param page   Map of Pages the alias might be pointing to
  *               pointing to the alias count (how often the alias points to the page)
  *               Map(page -> alias count)
  * @param offset character offset of this alias in the plain text of the page it appears in
  */
case class ExtendedLink(
	alias: String,
	var page: Map[String, Int] = Map[String, Int](),
	var offset: Option[Int] = None
) {
	/**
	  * Filters Pages of an extended link.
	  *
	  * @param countThresh the number of times the alias at least has to point to the links page
	  * @param normalizedThresh the number the biggest alias count has to be bigger than the others
	  * @return One page or None
	  */
	def filterExtendedLink(countThresh: Int, normalizedThresh: Double): Option[String] = {
		page.size match {
			case 0 | 1 => page.headOption.map(_._1)
			case _ =>
				val max = page.values.max
				val filteredPages = page
					.filter { case (page, count) =>
						val normalized = count.toDouble / max
						count > countThresh || normalized > normalizedThresh
					}
				filteredPages.size match {
					case 0 | 1 => page.headOption.map(_._1)
					case _ => None
				}
		}
	}
}
