// Every once in a while run `sbt dependencyUpdates` and `sbt dependencyCheckAggregate` here
import Tests._

import scala.io.Source

enablePlugins(GitVersioning, S3Plugin, GitBranchPrompt)
git.useGitDescribe := true

val sparkVersion = "2.3.0"
val elastic4sVersion = "6.2.4"
val playVersion = "2.6.8"
val emrProvidedAwsSdkVersion = "1.11.160"
val emrProvidedHadoopVersion = "2.7.3"
val sparkScalaVersion = "2.11.8" // Spark relies on a specific version of Scala (including for some hacks)
val playExclusionRules = Seq(
  ExclusionRule("com.fasterxml.jackson.core", "jackson-databind"),
  ExclusionRule("com.google.guava", "guava"),
  ExclusionRule("org.slf4j", "slf4j-api"),
  ExclusionRule("org.slf4j", "jcl-over-slf4j"),
  ExclusionRule("com.typesafe.play", "build-link")
)
val scalatestExclusionRules = Seq(
  ExclusionRule("org.scalatest", "scalatest"),
  ExclusionRule("org.scalactic", "scalactic")
)

lazy val defaultSettings = Seq(
  name := "endor-blockchain-data-pipeline",
  organization := "com.endor",
  scalaVersion := sparkScalaVersion,

  javaOptions ++= Seq("-Xms512M", "-Xmx8192M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),

  scalacOptions ++= Seq(
    "-feature", "-deprecation", "-unchecked", "-explaintypes",
    "-encoding", "UTF-8", // yes, this is 2 args
    "-language:reflectiveCalls", "-language:implicitConversions", "-language:postfixOps", "-language:existentials",
    "-language:higherKinds",
    // http://blog.threatstack.com/useful-scala-compiler-options-part-3-linting
    "-Xcheckinit", "-Xexperimental", "-Xfatal-warnings", /*"-Xlog-implicits", */"-Xfuture", "-Xlint",
    "-Ywarn-dead-code", "-Ywarn-inaccessible", "-Ywarn-numeric-widen", "-Yno-adapted-args", "-Ywarn-unused-import",
    "-Ywarn-unused"
  ),

  wartremoverErrors ++= Seq(
    Wart.StringPlusAny, Wart.FinalCaseClass, Wart.JavaConversions, Wart.Null, Wart.Product, Wart.Serializable,
    Wart.LeakingSealed, Wart.While, Wart.Return, Wart.ExplicitImplicitTypes, Wart.Enumeration, Wart.FinalVal,
    Wart.TryPartial, Wart.TraversableOps, Wart.OptionPartial, ContribWart.SomeApply
  ),

  wartremoverWarnings ++= wartremover.Warts.allBut(
    Wart.Nothing, Wart.DefaultArguments, Wart.Throw, Wart.MutableDataStructures, Wart.NonUnitStatements, Wart.Overloading,
    Wart.Option2Iterable, Wart.ImplicitConversion, Wart.ImplicitParameter, Wart.Recursion,
    Wart.Any, Wart.Equals, // Too many warnings because of spark's Row
    Wart.AsInstanceOf, // Too many warnings because of bad DI practices
    Wart.ArrayEquals // Too many warnings because we're using byte arrays in Spark
  ),

  testFrameworks := Seq(TestFrameworks.ScalaTest),
  logBuffered in Test := false,

  scalaVersion := sparkScalaVersion,

  resolvers ++= Seq(
    Resolver.mavenLocal,
    Resolver.sonatypeRepo("public"),
    Resolver.typesafeRepo("releases"),
    "jitpack" at "https://jitpack.io",
    "ethereum" at "https://dl.bintray.com/ethereum/maven/"
  ),

  // This needs to be here for Coursier to be able to resolve the "tests" classifier, otherwise the classifier's ignored
  classpathTypes += "test-jar",
  resourceGenerators in Test += Def.task {
    def getResourceContents(classpathEntry: Attributed[File], resourceName: String): Option[String] = {
      classpathEntry.get(artifact.key).map(entryArtifact => {
        val jarFile = classpathEntry.data
        IO.withTemporaryDirectory { tmpDir =>
          IO.unzip(jarFile, tmpDir)
          // copy to project's target directory
          // Instead of copying you can do any other stuff here
          Source.fromFile(tmpDir / resourceName).mkString
        }
      })
    }
    val contents = (dependencyClasspath in Test).value
      .filter(_.get(artifact.key) match {
        case Some(artifactKey) if artifactKey.name == "ethereumj-core" => true
        case Some(artifactKey) if artifactKey.extraAttributes.get("groupId").contains("org.web3j") && artifactKey.name == "core" => true
        case _ => false
      })
      .flatMap(entry => getResourceContents(entry, "version.properties"))
      .mkString(System.lineSeparator())
    val file = (resourceManaged in Compile).value / "version.properties"
    IO.write(file, contents)
    Seq(file)
  }.taskValue
)

lazy val assemblySettings = Seq(
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  test in assembly := {},

  assemblyMergeStrategy in assembly := {
    case x if x.endsWith("application.conf") => MergeStrategy.first
    case x if x.endsWith(".class") => MergeStrategy.last
    case x if x.endsWith("logback.xml") => MergeStrategy.first
    case x if x.endsWith("version.properties") => MergeStrategy.concat
    case x if x.endsWith(".properties") => MergeStrategy.last
    case x if x.contains("/resources/") => MergeStrategy.last
    case x if x.startsWith("META-INF/mailcap") => MergeStrategy.last
    case x if x.startsWith("META-INF/mimetypes.default") => MergeStrategy.first
    case x if x.startsWith("META-INF/maven/org.slf4j/slf4j-api/pom.") => MergeStrategy.first
    case x if x.startsWith("CHANGELOG.") => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      if (oldStrategy == MergeStrategy.deduplicate)
        MergeStrategy.first
      else
        oldStrategy(x)
  }
)

lazy val `serialization` = project.in(file("libraries/serialization"))
  .settings(defaultSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.joda"                    % "joda-convert"    % "1.9.2",
      "com.typesafe.play"          %% "play-json"       % playVersion   % "provided",
      "com.typesafe.play"          %% "play-json-joda"  % playVersion   % "provided",
      "org.scalatest"              %% "scalatest"       % "3.0.4"       % "test"
    )
  )

lazy val `jobnik-client` = project.in(file("libraries/jobnik-client"))
  .dependsOn(`serialization`)
  .settings(defaultSettings)
  .settings(
    resolvers ++= Seq(
      Resolver.mavenLocal,
      Resolver.sonatypeRepo("public"),
      Resolver.typesafeRepo("releases")
    ),

    libraryDependencies ++= Seq(
      "com.amazonaws"        % "aws-java-sdk"                 % emrProvidedAwsSdkVersion  % "provided,test",
      "com.google.guava"     % "guava"                        % "11.0.2"                  % "provided",

      "net.debasishg"       %% "redisclient"                  % "3.4",
      "org.slf4j"            % "slf4j-api"                    % "1.7.25",
      "com.typesafe.play"   %% "play-json"                    % playVersion               excludeAll(playExclusionRules:_*),

      "ch.qos.logback"       % "logback-classic"              % "1.2.3"                   % "test",
      "org.scalatest"       %% "scalatest"                    % "3.0.4"                   % "test"
    )
  )

lazy val pipeline = project.in(file("pipeline"))
  .dependsOn(`jobnik-client`, serialization)
  .settings(defaultSettings ++ assemblySettings)
  .settings(
    // Allow parallel execution of tests as long as each of them gets its own JVM to create a SparkContext on (see SPARK-2243)
    fork in Test := true,
    testGrouping in Test := (definedTests in Test)
      .value
      .map(test => Group(test.name, Seq(test), SubProcess(ForkOptions()))),

    testOptions in Test := Seq()
  )
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark"             %% "spark-core"                      % sparkVersion              % "provided,test",
    "org.apache.spark"             %% "spark-sql"                       % sparkVersion              % "provided,test",
    "org.apache.spark"             %% "spark-hive"                      % sparkVersion              % "provided,test",
    "org.apache.spark"             %% "spark-catalyst"                  % sparkVersion              % "provided,test",
    "com.amazonaws"                 % "aws-java-sdk"                    % emrProvidedAwsSdkVersion  % "provided,test",
    "org.apache.hadoop"             % "hadoop-aws"                      % emrProvidedHadoopVersion  % "provided,test",
    "com.github.EndorCoin"          % "spark-blockchain-datasource"     % "59b3b74d1a449b59a122f566832d9bb0d0569208",
    "org.elasticsearch"             % "elasticsearch-hadoop"            % "6.2.4",
    "com.sksamuel.elastic4s"       %% "elastic4s-core"                  % elastic4sVersion,
    "com.sksamuel.elastic4s"       %% "elastic4s-http"                  % elastic4sVersion,
    "net.debasishg"                %% "redisclient"                     % "3.4",
    "com.typesafe.play"            %% "play-json"                       % playVersion               excludeAll(playExclusionRules:_*),
    "io.logz.logback"               % "logzio-logback-appender"         % "1.0.17"                  exclude("com.google.guava", "guava"),
    "ch.qos.logback"                % "logback-classic"                 % "1.2.3",
    "net.ruippeixotog"             %% "scala-scraper"                   % "2.0.0",

    "org.apache.spark"             %% "spark-core"                      % sparkVersion              % "test" classifier "tests",
    "org.apache.spark"             %% "spark-sql"                       % sparkVersion              % "test" classifier "tests",
    "org.apache.spark"             %% "spark-catalyst"                  % sparkVersion              % "test" classifier "tests",
    "org.scalatest"                %% "scalatest"                       % "3.0.4"                   % "test",
    "com.sksamuel.elastic4s"       %% "elastic4s-testkit"               % elastic4sVersion          % "test",
    "com.sksamuel.elastic4s"       %% "elastic4s-embedded"              % elastic4sVersion          % "test"
  ))

lazy val deploy = taskKey[Seq[String]]("Deploy fat JAR to S3")

lazy val root = project.in(file("."))
  .settings(defaultSettings)
  .settings(
    s3Progress in s3Upload := true,
    s3Host in s3Upload := "endor-coin-ci-artifacts.s3.amazonaws.com",
    deploy := (s3Upload dependsOn (assembly in (pipeline, assembly))).value,
    mappings in s3Upload := {
      val jarPath = (assemblyOutputPath in (pipeline, assembly)).value
      val codeVersion = version.value
      val minorVersion = codeVersion.split("\\.").take(2).mkString(".")
      Seq((jarPath, s"pipeline/$minorVersion/$codeVersion/pipeline-$codeVersion.jar"))
      Seq.empty
    }
)
  .aggregate(pipeline, `jobnik-client`, `serialization`)
