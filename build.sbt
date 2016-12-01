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
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)

}

excludeFilter in unmanagedJars := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"

//excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
//  val excludes = Set()
//  cp filter { jar => excludes(jar.data.getName) }
//}


// https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "2.1.0"

libraryDependencies += "org.apache.logging.log4j" % "log4j-1.2-api" % "2.4.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.4.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.4.1"