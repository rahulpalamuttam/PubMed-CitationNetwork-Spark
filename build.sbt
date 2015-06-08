name := "PubMed-CitationNetwork-Spark"

version := "1.0"

//organization := "org.biocaddie.citation.network"

mainClass in Compile := Some("Main")

javaHome := Some(file("/usr/java/jdk1.7.0_79"))

resolvers ++= Seq(
  // other resolvers here
  "Bintray sbt plugin releases" at "http://dl.bintray.com/sbt/sbt-plugin-releases/",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"
)


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.1.0",
  "org.apache.spark" % "spark-sql_2.10" % "1.1.0",
  "org.apache.spark" %% "spark-mllib" % "1.1.0",
  "org.apache.spark" % "spark-graphx_2.10" % "1.1.0"
)


// or 2.11.5
//scalaVersion := "2.11.6"
scalaVersion := "2.10.4"
    