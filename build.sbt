name := "ExoNode"

organization := "growin"

version := "1.3-SNAPSHOT"

scalaVersion := "2.12.2"

scalacOptions ++= Seq("-feature", "-deprecation")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1"

libraryDependencies ++= Seq("org.scala-lang.modules" % "scala-xml_2.12" % "1.0.6",
  "org.scala-lang" % "scala-reflect" % "2.12.2")

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "com.flyobjectspace" %% "flyscala" % "2.2.0-SNAPSHOT"

libraryDependencies += "io.swave" %% "swave-core" % "0.7.0"

libraryDependencies ++= Seq(
  compilerPlugin("com.github.wheaties" % "twotails" % "0.3.1" cross CrossVersion.fullMapped(mapVersions)),
  "com.github.wheaties" % "twotails-annotations" % "0.3.1" cross CrossVersion.fullMapped(mapVersions)
)

def mapVersions(v: String) = v match {
  case "2.12.2" => "2.12.0"
  case x => x
}