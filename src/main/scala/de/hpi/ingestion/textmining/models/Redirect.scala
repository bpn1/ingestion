package de.hpi.ingestion.textmining.models

/**
  * Case class representing a Wikipedia page and all aliases that link to this page.
  *
  * @param alias redirect page
  * @param page page to which is redirected
  */
case class Redirect(alias: String, page: String)
