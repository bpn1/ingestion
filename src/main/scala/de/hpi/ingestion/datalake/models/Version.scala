package de.hpi.ingestion.datalake.models

import java.util.{Date, UUID}

import com.datastax.driver.core.utils.UUIDs

case class Version(
	version: UUID = UUIDs.timeBased(),
	var program: String,
	var value: List[String] = List[String](),
	var validity: Map[String, String] = Map[String, String](),
	var datasources: List[String] = List[String](),
	var timestamp: Date = new Date()
)
