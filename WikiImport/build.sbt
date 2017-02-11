lazy val root = (project in file(".")).
	settings(
		name := "WikiImport",
		version := "1.0",
		scalaVersion := "2.11.8",
		mainClass in Compile := Some("WikipediaImport")
	)

resolvers ++= Seq(
	"Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
	"Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
)

libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-core_2.11" % "2.1.0",
	"org.apache.spark" % "spark-sql_2.11" % "2.1.0",
	"com.typesafe.play" %% "play-json" % "2.3.0",
	"com.databricks" % "spark-xml_2.11" % "0.4.1",
	"com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.0-M3",
	"com.datastax.cassandra" % "cassandra-driver-core" % "3.1.3",
	"org.scalactic" % "scalactic_2.11" % "3.0.1",
	"org.scalatest" % "scalatest_2.11" % "3.0.1" % "test",
	"com.holdenkarau" % "spark-testing-base_2.11" % "2.1.0_0.6.0"
)

// testing settings
logBuffered in Test := false
parallelExecution in Test := false
fork in Test := true
testOptions in Test := Seq(Tests.Argument(TestFrameworks.ScalaTest, "-oD"), Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports"))
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")

// fat jar assembly settings
assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case PathList(ps @ _*) if ps.last endsWith "pom.properties" => MergeStrategy.discard
	case _ => MergeStrategy.first
}

assemblyShadeRules in assembly := Seq(
	ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)

// to suppress include info and merge warnings
logLevel in assembly := Level.Error
