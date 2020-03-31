import NativePackagerHelper._

name := "exercises"

version := "0.1"


name := "SparkSbtSkel"
organization := "io.elegans"
maintainer := "angelo.leto@elegans.io"

crossScalaVersions := Seq("2.12.10")

resolvers ++= Seq("Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/",
                  Resolver.bintrayRepo("hseeberger", "maven"))

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
	"org.apache.spark" %% "spark-mllib" % "2.4.5" % "provided",
	"org.elasticsearch" % "elasticsearch-hadoop" % "7.6.0",
	"edu.stanford.nlp" % "stanford-corenlp" % "3.9.2",
	"edu.stanford.nlp" % "stanford-corenlp" % "3.9.0" classifier "models",
	"com.github.scopt" %% "scopt" % "3.7.0"
)

enablePlugins(JavaServerAppPackaging)

// Assembly settings
mainClass in Compile := Some("io.elegans.exercises.TokenizeSentences")
mainClass in assembly := Some("io.elegans.exercises.TokenizeSentences")

mappings in Universal ++= {
  // copy configuration files to config directory
  directory("scripts")
}

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
}

licenses := Seq(("GPLv3", url("https://opensource.org/licenses/MIT")))

