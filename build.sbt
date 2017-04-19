import sbtassembly.AssemblyPlugin.autoImport._

name := "solar"
version := "1.0"
scalaVersion := "2.11.7"
mainClass in Compile := Some("com.Main")
assemblyJarName := "solar_assembly.jar"
autoScalaLibrary := true
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
resolvers += Resolver.mavenLocal
resolvers += Resolver.defaultLocal
resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
resolvers += "Local ivy Repository" at "file://"+Path.userHome.absolutePath+"/.ivy2/cache"
resolvers += "HortonWorks Public-2" at "http://repo.hortonworks.com/content/repositories/releases/"
resolvers += "HortonWorks Public-3" at "http://repo.hortonworks.com/content/groups/public/"
resolvers += "HortonWorks Public-1" at "http://repo.hortonworks.com/content/repositories/re-hosted/"
resolvers += "HortonWorks Public-4" at "http://repo.hortonworks.com/content/repositories/jetty-hadoop/"
resolvers += "Apache Repository" at "http://central.maven.org/maven2/"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.0.2.5.0.0-1245" % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "2.0.0.2.5.0.0-1245" ,
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.0.0.2.5.0.0-1245" ,
  "org.apache.kafka" % "kafka-clients" % "0.10.0.2.5.0.0-1245",
  "org.apache.hbase" % "hbase-client" % "1.1.2.2.5.0.0-1245" ,
  "org.apache.hbase" % "hbase-common" % "1.1.2.2.5.0.0-1245" ,
  "com.google.protobuf" % "protobuf-java" % "2.6.1",
  "mysql" % "mysql-connector-java" % "5.1.39",
  "org.elasticsearch.client" % "transport" % "5.2.1",
  "org.apache.commons" % "commons-pool2" % "2.4.2",
  "redis.clients" % "jedis" % "2.9.0",
  "org.apache.commons" % "commons-configuration2" % "2.1.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.8.1",
  "org.apache.logging.log4j" % "log4j-api" % "2.8.1"
)
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
   cp filter {_.data.getName == "scalactic_2.11-2.2.5.jar"}
}
assemblyMergeStrategy in assembly := {
  case PathList("org", "joda", "time", "base", "BaseDateTime.class") => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".RSA" => MergeStrategy.first
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("org.scalactic", "scalactic_2.11", xs @ _*) => MergeStrategy.last
  case PathList("mynetty", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "META-INF/io.netty.versions.properties" =>MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "mygoogle.@1").inAll,
    ShadeRule.rename("io.netty.**" -> "mynetty.@1").inAll
)

