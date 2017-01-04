lazy val root = (project in file(".")).
  settings(
    name := "VersionControl",
    version := "1.0",
    scalaVersion := "2.10.5",
    mainClass in Compile := Some("WikipediaImport")
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
  "com.typesafe.play" %% "play-json" % "2.3.0"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith "pom.properties" => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)
