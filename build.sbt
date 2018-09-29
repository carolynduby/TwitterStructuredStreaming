name := "twitterStream"
 
version := "0.1"
 
scalaVersion := "2.11.12"
 
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1.3.0.0.0-1634"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.1.3.0.0.0-1634"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.3.1.3.0.0.0-1634"

 
resolvers := List("Hortonworks Releases" at "http://repo.hortonworks.com/content/repositories/releases/", "Jetty Releases" at "http://repo.hortonworks.com/content/repositories/jetty-hadoop/")

assemblyMergeStrategy in assembly := {
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "git.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

target in assembly := file("build")

/* including scala bloats your assembly jar unnecessarily, and may interfere with
   spark runtime */
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}.jar" 
