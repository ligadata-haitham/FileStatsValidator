name := "FileStatsValidator"

version := "1.0"

scalaVersion := "2.11.8"



//assemblyOption in assembly ~= {
//  _.copy(prependShellScript = Some(defaultShellScript))
//}

assemblyJarName in assembly := {
  s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"
}

assemblyMergeStrategy in assembly := {
  case x if x contains "package-info.class" => MergeStrategy.last
  case x if x contains "plugin.xml" => MergeStrategy.last
  case x if x contains "Log4j2Plugins.dat" => MergeStrategy.last
  case x if x contains "jackson" => MergeStrategy.last
  case x if x contains "thrift" => MergeStrategy.last
  case x if x contains "apache/hive/common" => MergeStrategy.last
  case x if x contains "apache\\hive\\common" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)

}

excludeFilter in unmanagedJars := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  val excludes = Set("slider-core-0.90.2-incubating.jar", "datanucleus-core-4.1.6.jar", "ant-1.6.5.jar", "log4j-1.2.17.jar", "apache-log4j-extras-1.2.17.jar", "log4j-1.2.16.jar", "jsp-2.1-6.1.14.jar", "commons-beanutils-1.7.0.jar", "jsp-api-2.0.jar", "servlet-api-2.5.jar", "log4j-slf4j-impl-2.4.1.jar", "jdo-api-3.0.1.jar", "commons-beanutils-core-1.8.0.jar", "geronimo-jta_1.1_spec-1.1.1.jar", "jsp-api-2.1-6.1.14.jar", "servlet-api-2.5-6.1.14.jar")
  cp filter { jar => excludes(jar.data.getName) }
}


// https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc
//libraryDependencies += "org.apache.hive" % "hive-jdbc" % "2.1.0"

resolvers += "pentaho-aggdesigner-algorithm" at "http://repo.spring.io/libs-release/"
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0"
libraryDependencies += "org.apache.hive" % "hive-exec" % "1.1.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"



libraryDependencies += "org.apache.logging.log4j" % "log4j-1.2-api" % "2.4.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.4.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.4.1"
