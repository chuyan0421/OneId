name := "OneId"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.3"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.6.3"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.3"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.0"


mainClass in (Compile, run) := Some("OneId")
mainClass in (Compile, packageBin) := Some("OneId")