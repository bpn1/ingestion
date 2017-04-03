package de.hpi.ingestion.datalake.models

import java.util.{Date, UUID}

case class Version(
	version: UUID,
	program: String,
	value: List[String],
	validity: Map[String, String],
	datasources: List[String],
	timestamp: Date
)
