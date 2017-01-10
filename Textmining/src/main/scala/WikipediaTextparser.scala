import org.sweble.wikitext._
import org.sweble.wikitext.engine._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.sweble.wikitext.engine.utils._

object WikipediaTextparser {
    val keyspace = "wikidumps"
	val tablename = "wikipedia"

    case class WikipediaEntry(
    	title: String,
    	page: String
    )

    def parseWikipediaPage(entry : WikipediaEntry): EngPage = {
        val config = DefaultConfigEnWp.generate()
		val compiler = new WtEngineImpl(config)
		val pageTitle = PageTitle.make(config, entry.title)
		val pageId = new PageId(pageTitle, 0)
		val page = compiler.postprocess(pageId, entry.page, null).getPage
        println(page)
        page
    }

    def main(args : Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("VersionDiff")
			.set("spark.cassandra.connection.host", "172.20.21.11")
		val sc = new SparkContext(conf)


    	val wikipedia = sc.cassandraTable[WikipediaEntry](keyspace, tablename)
        wikipedia.map(parseWikipediaPage)

		sc.stop
    }

}
