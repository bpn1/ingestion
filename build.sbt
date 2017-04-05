lazy val root = (project in file(".")).
	settings(
		name := "ingestion",
		version := "1.0",
		scalaVersion := "2.11.8",
		mainClass in Compile := Some("ingestion")
	)

resolvers ++= Seq(
	"Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
)

libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-core_2.11" % "2.1.0" exclude("org.scalatest", "scalatest_2.11"),
	"com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.1",
	"org.apache.spark" % "spark-sql_2.11" % "2.1.0",
	"org.scalactic" % "scalactic_2.11" % "3.0.1",
	"org.scalatest" % "scalatest_2.11" % "3.0.1" % "test",
	"com.holdenkarau" % "spark-testing-base_2.11" % "2.1.0_0.6.0",
	"com.databricks" % "spark-xml_2.11" % "0.4.1",
	"info.bliki.wiki" % "bliki-core" % "3.1.0" exclude("ch.qos.logback", "logback-classic"),
	"org.jsoup" % "jsoup" % "1.10.2",
	"com.esotericsoftware" % "kryo" % "4.0.0",
	"com.google.protobuf" % "protobuf-java" % "2.6.1",
	"org.apache.lucene" % "lucene-analyzers-common" % "6.5.0",
	"com.typesafe.play" %% "play-json" % "2.4.11",
	"com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.4"
)

// testing settings
logBuffered in Test := false
parallelExecution in Test := false
fork in Test := true
testOptions in Test := Seq(Tests.Argument(TestFrameworks.ScalaTest, "-oD"), Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports"))
javaOptions ++= Seq("-Xms512M", "-Xmx4096M", "-XX:+CMSClassUnloadingEnabled")

// fat jar assembly settings
assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case PathList(ps @ _*) if ps.last endsWith "pom.properties" => MergeStrategy.discard
	case _ => MergeStrategy.first
}

// disables testing for assembly
test in assembly := {}

// to suppress include info and merge warnings
logLevel in assembly := Level.Error

// scalastyle config
scalastyleSources in Compile ++= (unmanagedSourceDirectories in Test).value

// ScalaDoc Settings
scalacOptions in (Compile, doc) ++= Seq("-doc-footer", "Impressum: https://hpi.de/naumann/sites/ingestion/impressum/")
scalacOptions in (Compile, doc) ++= Seq("-doc-source-url", "https://github.com/bpn1/ingestion/tree/master/src/main/scala/€{FILE_PATH}")
