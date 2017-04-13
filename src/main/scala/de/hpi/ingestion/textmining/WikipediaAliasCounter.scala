package de.hpi.ingestion.textmining

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.textmining.models._

object WikipediaAliasCounter {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val inputLinksTablename = "wikipedialinks"
	val outputTablename = "wikipedialinks"
	val maximumAliasLength = 1000

	/**
	  * Calculates the probability that an alias is a link.
	  *
	  * @param aliasCounter Alias of a given alias
	  * @return percentage of the occurrences as link.
	  */
	def probabilityIsLink(aliasCounter: Alias): Double = {
		aliasCounter.link_occurrences.toDouble / aliasCounter.total_occurrences
	}

	/**
	  * Extracts list of link and general alias occurrences for an article
	  *
	  * @param entry article from which the aliases will be extracted
	  * @return list of aliases each with an occurrence set to 1
	  */
	def extractAliasList(entry: ParsedWikipediaEntry): List[Alias] = {
		val linkSet = entry.allLinks()
			.map(_.alias)
			.toSet

		val links = linkSet
			.toList
			.map(Alias(_, Map(), 1, 1))

		val aliases = entry.foundaliases
			.toSet
			.filterNot(linkSet)
			.toList
			.map(Alias(_, Map(), 0, 1))

		links ++ aliases
	}

	/**
	  * Reduces two AliasCounters of the same alias.
	  *
	  * @param alias1 first Alias with the same alias as alias2
	  * @param alias2 second Alias with the same alias as alias1
	  * @return Alias with summed link and total occurrences
	  */
	def aliasReduction(alias1: Alias, alias2: Alias): Alias = {
		Alias(
			alias1.alias,
			Map(),
			alias1.link_occurrences + alias2.link_occurrences,
			alias1.total_occurrences + alias2.total_occurrences)
	}

	/**
	  * Counts link and total occurrences of link aliases in Wikipedia articles.
	  *
	  * @param articles RDD containing parsed wikipedia articles
	  * @return RDD containing alias counts for links and total occurrences
	  */
	def countAliases(articles: RDD[ParsedWikipediaEntry]): RDD[Alias] = {
		articles.flatMap(extractAliasList)
			.map(alias => (alias.alias, alias))
			.reduceByKey(aliasReduction(_, _))
			.map(_._2)
			.filter(_.alias.nonEmpty)
	}

	/**
	  * Joins the alias counts and links
	  *
	  * @param aliasCounts aliases with their respective numbers of occurrences
	  * @param links       links with all their linked pages
	  * @return links with their alias counts
	  */
	def mergeAliasCountsAndLinks(aliasCounts: RDD[Alias], links: RDD[Alias]): RDD[Alias] = {
		val joinableLinks = links
			.map(link => (link.alias, link))

		aliasCounts
			.map(aliasCount => (aliasCount.alias, aliasCount))
			.leftOuterJoin(joinableLinks)
			.map {
				case (alias, (aliasCount, Some(link))) =>
					Alias(alias, link.pages, aliasCount.link_occurrences, aliasCount.total_occurrences)
				case (alias, (aliasCount, None)) =>
					Alias(alias, Map(), aliasCount.link_occurrences, aliasCount.total_occurrences)
			}
	}

	/**
	  * Counts alias occurrences and merges them into previously extracted aliases with their corresponding pages
	  *
	  * @param articles parsed Wikipedia articles
	  * @param links    articles with their corresponding pages
	  * @return merged links and alias counts
	  */
	def run(articles: RDD[ParsedWikipediaEntry], links: RDD[Alias]): RDD[Alias] = {
		val aliasCounts = countAliases(articles)
			.cache
			.filter(_.alias.length <= maximumAliasLength)
		mergeAliasCountsAndLinks(aliasCounts, links)
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Alias Counter")

		val sc = new SparkContext(conf)

		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		val links = sc.cassandraTable[Alias](keyspace, inputLinksTablename)
		run(articles, links).saveToCassandra(keyspace, outputTablename)

		sc.stop()
	}
}
