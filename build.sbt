name := "ExoNode"

organization := "growin"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1"

libraryDependencies ++=  Seq("org.scala-lang.modules" % "scala-xml_2.12" % "1.0.6",
                             "org.scala-lang" % "scala-reflect" % "2.12.1")

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq("com.flyobjectspace" %% "flyscala" % "2.2.0-SNAPSHOT")
