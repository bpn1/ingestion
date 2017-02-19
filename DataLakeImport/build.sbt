lazy val root = (project in file(".")).
  settings(
    name := "DataLakeImport",
    version := "1.0",
    scalaVersion := "2.11.8",
    mainClass in Compile := Some("DataLakeImport")
  )

exportJars := true

resolvers ++= Seq(
  "Spark Packages" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
  "com.datastax.spark" % "spark-cassandra-connector_2.10" % "2.0.0-M3",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.3",
  "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.4",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0"
)

// testing settings
logBuffered in Test := false
parallelExecution in Test := false
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

// fat jar assembly settings
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith "pom.properties" => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)

// scalastyle config file
scalastyleConfig := new File("../scalastyle-config.xml")

