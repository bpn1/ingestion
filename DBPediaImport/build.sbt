lazy val root = (project in file(".")).
  settings(
    name := "DBPediaImport",
    version := "1.1",
    scalaVersion := "2.10.5",
    mainClass in Compile := Some("DBPediaDumpURLs")
  )

resolvers ++= Seq(
  "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
  "Spark Packages" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies ++= Seq(
  "org.jsoup" % "jsoup" % "1.10.1",
  "org.apache.spark" %% "spark-core" % "1.6.2",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.2",
  // "org.apache.jena" % "jena-core" % "3.1.1",
  // "org.apache.jena" % "jena" % "3.1.1",
  // "org.apache.jena" % "jena-tdb" % "3.1.1",
  // "org.apache.jena" % "jena-sdb" % "3.1.1",
  // "org.apache.jena" % "apache-jena-libs" % "3.1.1",
  // "org.apache.jena" % "jena-elephas-io" % "3.1.1",
  // "org.apache.jena" % "jena-elephas-mapreduce" % "3.1.1",
  // "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  // "org.apache.hadoop" % "hadoop-mapreduce-client-common" % "2.6.0",
  "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.6.1",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.2"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case PathList("META-INF", "services", "org.apache.jena.system.JenaSubsystemLifecycle") => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "pom.properties" => MergeStrategy.discard
  case x => MergeStrategy.first
}}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)
