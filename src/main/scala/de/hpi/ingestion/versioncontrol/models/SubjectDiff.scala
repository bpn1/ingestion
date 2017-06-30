package de.hpi.ingestion.versioncontrol.models

import java.util.UUID

case class SubjectDiff(
	oldversion: UUID,
	newversion: UUID,
	id: UUID,
	var master: Option[String] = None,
	var datasource: Option[String] = None,
	var aliases: Option[String] = None,
	var category: Option[String] = None,
	var name: Option[String] = None,
	var properties: Option[String] = None,
	var relations: Option[String] = None
)
