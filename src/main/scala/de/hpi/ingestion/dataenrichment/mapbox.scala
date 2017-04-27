package de.hpi.ingestion.dataenrichment

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.deduplication.similarity._
import de.hpi.ingestion.deduplication.models._
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs
import play.api.mvc.Action

import scala.xml.XML
import scala.collection.mutable
import scala.io.Source
import scala.util.parsing.json.JSONObject

//import com.mapbox.services.api.geocoding.v5._
//import com.mapbox.services.commons.models.Position
import com.ning.http.client.AsyncHttpClientConfig
import play.api.libs.ws.ning.NingWSClient
import scala.concurrent.ExecutionContext.Implicits.global
import pla.api.libs
import scala.concurrent.Future
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import scala.concurrent.duration._
import play.api.libs.concurrent.Timeout


class MapboxDataEnrichment() extends Serializable {



	def getGeoCode(request: String) : scala.concurrent.Future[play.api.libs.json.JsValue] = {
		val httpConfigBuilder = new AsyncHttpClientConfig.Builder()
		val httpConfig = httpConfigBuilder.build()
		val client = new NingWSClient(httpConfig)

/*		val output = client
			.url("https://api.mapbox.com/geocoding/v5/mapbox.places/strasse+5+9+13125+Berlin.json?access_token=pk.eyJ1IjoicmFkc2NoZWl0IiwiYSI6ImNqMXRoMnZ2aDAwMW0zM3FyMm9oMzJ0MTEifQ.xdZLGZA5FNwi97t2UC4ogw")
			.get()
			.map { res =>
				client.close()
				println(res.getStatusCode)
				res.json
			}
			def wb = Action.async{ request =>
    val aas = WS.url("http://localhost/").withRequestTimeout(1000).get

    aas.map(response =>{
         Ok("responded")
    })
}

			*/
		val url = "https://api.mapbox.com/geocoding/v5/mapbox.places/strasse+5+9+13125+Berlin.json?access_token=pk.eyJ1IjoicmFkc2NoZWl0IiwiYSI6ImNqMXRoMnZ2aDAwMW0zM3FyMm9oMzJ0MTEifQ.xdZLGZA5FNwi97t2UC4ogw"

		def wb = Action.async{ request =>
			val a = client.url(url).withRequestTimeout(1000).get
		}

		val futureResult: Future[play.api.libs.json.JsValue] = client.url(url).get().map {
		 response => (response.json)
	 	}
		println(futureResult)
		futureResult

	}

}

object MapboxDataEnrichment {
	val context = "Ingestion"
	//val Mapbox = MapboxBuilder.build()
	//Mapbox.getInstance(
	//	context,
	//	"pk.eyJ1IjoicmFkc2NoZWl0IiwiYSI6ImNqMXRoMnZ2aDAwMW0zM3FyMm9oMzJ0MTEifQ.xdZLGZA5FNwi97t2UC4ogw"
	//)

}