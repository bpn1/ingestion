package de.hpi.ingestion.framework.mock

import de.hpi.ingestion.framework.Configurable

class MockConfigurable(path: String = "test.xml") extends Configurable {
	configFile = path
}
