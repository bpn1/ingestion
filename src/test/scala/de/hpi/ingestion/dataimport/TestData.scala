package de.hpi.ingestion.dataimport

object TestData {
	def validURLs: List[String] = {
		List("http://google.com", "https://www.facebook.com", "http://www.youtube.de")
	}
	def invalidURLs: List[String] = {
		List("www.youtube.de", "isbn://1337", "NotAURL", "about:black", "youtube.com")
	}
}
