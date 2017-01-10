import org.sweble.wikitext._
import org.sweble.wikitext.engine._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.sweble.wikitext.engine.utils._
import org.sweble.wikitext.engine.nodes._
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.sweble.wikitext.parser.nodes._

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
        page
    }

    def parseTree(page : EngPage): (String, List[Map[String,String]]) = {
        var result = traversePageTree(page, 0)
        result = (result._1.replaceAll("\n", ""), result._2)
        println(result)
        result
    }

    // traverse all children
    def traverseChildren(node : WtNode, offset: Int): (String, List[Map[String,String]]) = {
        var text = ""
        val linkList = mutable.ListBuffer[Map[String, String]]()
        //println(s"Node: $node")
        if(node.iterator == null)
            return (text, linkList.toList)
        for(element <- node.iterator.toList) {
            val (resultText, resultList) = traversePageTree(element, offset + text.length)
            text += resultText
            linkList ++= resultList
        }
        (text, linkList.toList)
    }

    def traversePageTree(node : WtNode, offset : Int): (String, List[Map[String,String]]) = {
        var text = ""
        val linkList = mutable.ListBuffer[Map[String, String]]()
        node match {
            case t: EngPage => traverseChildren(t, offset)
            case t: WtParagraph => traverseChildren(t, offset)
            case t: WtTemplate =>
                // check [0], if Infobox discard
                // else ?
                //println(t)
                (text, linkList.toList)
            case t: WtText =>
                // extract text
                text += t.getContent
                (text, linkList.toList)
            case t: WtBold => traverseChildren(t, offset)
            case t: WtItalics => traverseChildren(t, offset)
            case t: WtInternalLink =>
                // [0] = target (WtPagename)
                // [1] = source text (WtLinkTitle or WtNoLinkTitle)
                val link = mutable.Map[String, String]()
                var source = ""
                var target = ""
                if(t.iterator != null) {
                    for(prop <- t.iterator.toList) {
                        prop match {
                            case p: WtPageName => target = p(0).asInstanceOf[WtText].getContent
                            case p: WtLinkTitle =>
                                val contentList = p.iterator.toList
                                if(contentList.length == 0) {
                                    source = ""
                                } else {
                                    source = contentList(0).asInstanceOf[WtText].getContent
                                }
                            case _ =>
                        }
                    }
                }

                if(source == "")
                    source = target
                link("source") = source
                link("target") = target
                link("char_offset_begin") = offset.toString
                link("char_offset_end") = (offset + source.length).toString
                linkList += link.toMap
                text += source
                (text, linkList.toList)
            //case t: WtExternalLink
            case _ =>
                println(s"\n\nfail: $node.iterator.toList \n\n")
                (text, linkList.toList)
        }
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
