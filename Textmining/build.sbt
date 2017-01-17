lazy val root = (project in file(".")).
	settings(
		name := "Textmining",
		version := "1.0",
		scalaVersion := "2.10.5",
		mainClass in Compile := Some("Textmining")
	)

resolvers ++= Seq(
	"Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven",
	"Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.6.2",
	"com.datastax.spark" %% "spark-cassandra-connector" % "1.6.1",
	"org.apache.spark" % "spark-sql_2.10" % "1.6.2",
	"com.datastax.cassandra" % "cassandra-driver-core" % "3.1.2",
	"com.typesafe.play" %% "play-json" % "2.3.0",
	"org.scalactic" %% "scalactic" % "3.0.1",
	"org.scalatest" %% "scalatest" % "3.0.1" % "test",
	"com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.3",
	"com.databricks" % "spark-xml_2.10" % "0.4.1",
	"info.bliki.wiki" % "bliki-core" % "3.1.0",
	"org.jsoup" % "jsoup" % "1.10.2"
)

// testing settings
logBuffered in Test := false
parallelExecution in Test := false
fork in Test := true
testOptions in Test := Seq(Tests.Argument(TestFrameworks.ScalaTest, "-oD"), Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports"))
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

// fat jar assembly settings
assemblyMergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
		case PathList("META-INF", xs @ _*) => MergeStrategy.discard
		case PathList(ps @ _*) if ps.last endsWith "pom.properties" => MergeStrategy.discard
		case x => MergeStrategy.first
	}
}

assemblyShadeRules in assembly := Seq(
	ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)
