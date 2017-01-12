import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, cassandra, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object WikipediaImport {
  val inputFile = "dewiki.xml" // load from hdfs

  val wikiSchema = StructType(Array(
    StructField("title", StringType),
    StructField("revision", StructType(Array(
      StructField("text", StringType))))))

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("WikipediaImport")
      .set("spark.cassandra.connection.host", "172.20.21.11")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    var df = sql.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .schema(wikiSchema)
      .load(inputFile)

    df = df.withColumn("text", df("revision.text")).drop("revision")
    df
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "wikipedia", "keyspace" -> "wikidumps"))
      .mode(SaveMode.Append)
      .save()

    sc.stop()
  }
}
