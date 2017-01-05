name := "ExoNode"

version := "1.0"

scalaVersion := "2.12.1"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq("com.flyobjectspace" %% "flyscala" % "2.2.0-SNAPSHOT")

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.14"