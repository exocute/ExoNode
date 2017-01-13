name := "ExoNode"

organization := "growin"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.1"

scalacOptions += "-feature"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq("com.flyobjectspace" %% "flyscala" % "2.2.0-SNAPSHOT")
