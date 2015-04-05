
name := """RecommenderService"""

version := "1.0"

scalaVersion := "2.10.4"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

externalResolvers := Resolver.withDefaultResolvers(resolvers.value, mavenCentral = false)

compileOrder := CompileOrder.JavaThenScala

resolvers ++= Seq(
  "inhouse" at "http://nexus.innomdc.com/nexus/content/groups/public/"
)

resolvers ++= Seq(
    "Spray repository" at "http://repo.spray.io",
    "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
)

resolvers += Resolver.sonatypeRepo("snapshots")

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

unmanagedBase <<= baseDirectory { base => base / "lib" }

resolvers ++= Seq(
            "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
            "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

libraryDependencies ++= Seq(
    "com.google.guava" % "guava" % "16.0" % "provided",
    "org.apache.spark" % "spark-mllib_2.10" % "1.2.0" exclude("com.google.guava", "guava"),
    "com.datastax.spark" %% "spark-cassandra-connector" %  "1.2.0-alpha1" ,
    "org.apache.cassandra" % "cassandra-all" % "2.1.1"  exclude("com.google.guava", "guava") ,
	"org.apache.cassandra" % "cassandra-thrift" % "2.1.1"  exclude("com.google.guava", "guava") ,
    "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.2"  exclude("com.google.guava", "guava") ,
    "org.apache.spark" %% "spark-core" % "1.2.0" % "provided" exclude("com.google.guava", "guava") exclude("org.apache.hadoop", "hadoop-core"),
    "org.apache.spark" %% "spark-streaming" % "1.2.0" % "provided"  exclude("com.google.guava", "guava"),
    "org.apache.spark" %% "spark-catalyst"   % "1.2.0"  % "provided" exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core"),
    "org.apache.spark" %% "spark-sql" % "1.2.0" %  "provided" exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core"),
    "org.apache.spark" %% "spark-hive" % "1.2.0" % "provided" exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core"),    
    "org.apache.hadoop" % "hadoop-client" % "1.0.4" % "provided",
    "com.typesafe.slick" %% "slick" % "1.0.1",
    "com.typesafe.akka" % "akka-actor_2.10" % "2.3.6",
    "com.typesafe.akka" % "akka-slf4j_2.10" % "2.3.6",
    "com.typesafe" % "config" % "1.2.1",
    "ch.qos.logback" % "logback-classic" % "1.1.2",
    "com.github.nscala-time" %% "nscala-time" % "1.0.0",
    "org.scalatest" % "scalatest_2.10" % "2.1.3" % "test",
    "junit" % "junit" % "4.8.1" % "test",
    "net.jpountz.lz4" % "lz4" % "1.2.0" % "provided",
    "org.clapper" %% "grizzled-slf4j" % "1.0.2",
    "net.jpountz.lz4" % "lz4" % "1.2.0" % "provided",
    "org.scalanlp" %% "breeze" % "0.10",
    "org.scalanlp" %% "breeze-natives" % "0.10"
   )
