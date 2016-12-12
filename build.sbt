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
  case x if x contains "org/apache/hadoop/hive" => MergeStrategy.last
  case x if x contains "org\\apache" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)

}

excludeFilter in unmanagedJars := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
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
