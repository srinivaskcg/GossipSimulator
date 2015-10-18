name := "GossipProtocol"
 
version := "1.0"
 
scalaVersion := "2.11.7"
 
resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
 
libraryDependencies +=
  "com.typesafe.akka" %% "akka-actor" % "2.4-SNAPSHOT"
  
libraryDependencies +=
  "com.typesafe.akka" %% "akka-remote" % "2.4-SNAPSHOT"
