package de.hpi.ingestion.dataimport.dbpedia.models

/**
  * Case class for a directed relation from subject to object of value relation
  *
  * @param subjectentity subject of the relation
  * @param relationtype  type of the relation
  * @param objectentity  object of the relation
  */
case class Relation(subjectentity: String, relationtype: String, objectentity: String)
