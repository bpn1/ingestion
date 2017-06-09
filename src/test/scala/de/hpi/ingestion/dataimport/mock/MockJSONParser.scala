package de.hpi.ingestion.dataimport.mock

import de.hpi.ingestion.dataimport.JSONParser
import play.api.libs.json.JsValue

class MockJSONParser extends JSONParser[MockEntity] {
	override def fillEntityValues(json: JsValue): MockEntity = {
		MockEntity("", "", "")
	}
}
