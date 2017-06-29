package de.hpi.ingestion.versioncontrol.models

import java.util.UUID

case class SubjectDiff(
	oldVersion: UUID,
	newVersion: UUID,
	id: UUID,
	master: String,
	datasource: String,
	aliases: String,
	category: String,
	name: String,
	properties: String,
	relations: String
)