import org.sweble.wikitext._
import org.sweble.wikitext.engine._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.sweble.wikitext.engine.utils._
import org.sweble.wikitext.engine.nodes._
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.sweble.wikitext.parser.nodes._
import scala.util.matching.Regex

object WikipediaTextparser {
  val keyspace = "wikidumps"
  val tablename = "wikipedia"

  case class WikipediaEntry(
                             title: String,
                             var text: String,
                             var links: List[Map[String, String]]
                           )

  def parseWikipediaPage(entry: WikipediaEntry): EngPage = {
    val config = DefaultConfigEnWp.generate()
    val compiler = new WtEngineImpl(config)
    val pageTitle = PageTitle.make(config, entry.title)
    val pageId = new PageId(pageTitle, 0)
    val page = compiler.postprocess(pageId, entry.text, null).getPage
    page
  }

  def parseTree(page: EngPage): (String, List[Map[String, String]]) = {
    val result_raw = traversePageTree(page, 0)
    val result = (cleanText(result_raw._1), result_raw._2)
    println(s"\n\npage:\n") //$result")
    println(result._1)
    println()
    for (elem <- result._2) {
      println(elem)
    }
    println()
    result
  }

  // traverse all children
  def traverseChildren(node: WtNode, offset: Int): (String, List[Map[String, String]]) = {
    var text = ""
    val linkList = mutable.ListBuffer[Map[String, String]]()
    if (node.iterator == null)
      return (text, linkList.toList)
    for (element <- node.iterator.toList) {
      val (resultText, resultList) = traversePageTree(element, offset + text.length)
      text += resultText
      linkList ++= resultList
    }
    (text, linkList.toList)
  }

  // remove surplus spaces
  def cleanText(text: String): (String) = {
    text
      .replaceAll("^\\s+", "")
      .replaceAll("\\s{2,}", " ")
  }

  def traversePageTree(node: WtNode, offset: Int): (String, List[Map[String, String]]) = {
    val fileRegex = new Regex("^Datei:")
    val wikimarkupRegex = new Regex("(^\\[\\[.*\\]\\]$|(\\[\\[|\\]\\]))")
    var text = ""
    val linkList = mutable.ListBuffer[Map[String, String]]()

    node match {
      // try case t: EngPage | t: WtParagraph
      case t: EngPage => traverseChildren(t, offset)

      case t: WtParagraph => traverseChildren(t, offset)

      case t: WtBold => traverseChildren(t, offset)

      case t: WtItalics => traverseChildren(t, offset)

      case t: WtExternalLink => (text, linkList.toList)

      case t: WtXmlElement => (text, linkList.toList)

      case t: WtTagExtension => (text, linkList.toList)

      case t: WtTemplate =>
        // check [0], if Infobox discard
        // else ?
        (text, linkList.toList)

      case t: WtText =>
        // extract text
        var tmpText = t.getContent.replaceAll("\\\\n", " ")
        if (wikimarkupRegex.findFirstIn(tmpText) == None && fileRegex.findFirstIn(tmpText) == None)
          text += tmpText
        text
        (text, linkList.toList)

      case t: WtInternalLink =>
        // [0] = target (WtPagename)
        // [1] = source text (WtLinkTitle or WtNoLinkTitle)
        val link = mutable.Map[String, String]()
        var source = ""
        var target = ""
        if (t.iterator != null) {
          for (prop <- t.iterator.toList) {
            prop match {
              case p: WtPageName =>
                target = p(0).asInstanceOf[WtText].getContent
                if (fileRegex.findFirstIn(target) != None)
                  return (text, linkList.toList)
              case p: WtLinkTitle =>
                val contentList = p.iterator.toList
                for (textElement <- contentList)
                  source += textElement.asInstanceOf[WtText].getContent
              case _ =>
            }
          }
        }

        if (source == "")
          source = target
        link("source") = source
        link("target") = target
        link("char_offset_begin") = offset.toString
        link("char_offset_end") = (offset + source.length).toString
        linkList += link.toMap
        text += source
        (text, linkList.toList)

      case _ =>
        println(s"\n\nfail: $node.iterator.toList \n\n")
        (text, linkList.toList)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("VersionDiff")
      .set("spark.cassandra.connection.host", "172.20.21.11")
    val sc = new SparkContext(conf)

    val wikipedia = sc.cassandraTable[WikipediaEntry](keyspace, tablename)
    wikipedia
      .map { case wikipediaEntry =>
        val page = parseWikipediaPage(wikipediaEntry)
        val parsedData = parseTree(page)
        wikipediaEntry.text = parsedData._1
        wikipediaEntry.links = parsedData._2
        wikipediaEntry
      }.saveToCassandra(keyspace, tablename)
    sc.stop
  }
}
