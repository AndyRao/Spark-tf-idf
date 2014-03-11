import AssemblyKeys._ // put this at the top of the file

assemblySettings

name := "HotNewsSpark"

version := "1.0"

scalaVersion := "2.9.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.8.0-incubating" withSources() withJavadoc()

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "0.8.0-incubating" withSources() withJavadoc()

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "0.8.0-incubating" withSources() withJavadoc()

libraryDependencies += "org.mongodb" %% "casbah" % "2.6.2"

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
  }
}
