lazy val root = (project in file(".")).
  settings(
    name := "DBPediaImport",
    version := "1.1",
    scalaVersion := "2.10.5"
  )

resolvers ++= Seq(
  "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
  "Spark Packages" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies ++= Seq(
  "org.jsoup" % "jsoup" % "1.10.1",
  "org.apache.spark" %% "spark-core" % "1.6.2",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.2",
  "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.6.1",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.2",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.3"
)

logBuffered in Test := false
parallelExecution in Test := false
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case PathList("META-INF", "services", "org.apache.jena.system.JenaSubsystemLifecycle") => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "pom.properties" => MergeStrategy.discard
  case x => MergeStrategy.first
}}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)
