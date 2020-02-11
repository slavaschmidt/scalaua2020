package com.slasch

import org.apache.spark.sql.{functions => F, _}

object Ingestion extends App {

  def explodeAndCount(df: DataFrame)(col: String) = {
    count(explode(df)(col))(col)
  }

  def explode(df: DataFrame)(col: String) = {
    df.withColumn(col, F.split(F.col(col), "\\s*;\\s*"))
      .withColumn(col, F.explode(F.col(col)))
      .filter(_.getAs[String](col) != "NA")
      .filter(_.getAs[String](col) != "Other(s):")
  }

  def count(df: DataFrame)(col: String) = {
    df.withColumn("type", F.lit(col)).groupBy(col, "type", "year").count()
  }

  def combinations(spark: SparkSession, df: DataFrame)(cols: Seq[String]) = {
    import spark.implicits._
    val col = "combinations"
    val joined = cols.foldLeft(df) { (df, c) =>
      df.withColumn(col, F.concat(F.col(col), F.lit(";"), F.col(c)))
    }
    val splitted = joined
      .filter(_.getAs[String](col).nonEmpty)
      .withColumn(col, F.split(F.col(col), "\\s*;\\s*"))
      .withColumn(col, F.expr(s"""filter($col, x -> x != "Other(s):")"""))
      .withColumn(col, F.expr(s"""filter($col, x -> x != "N/A")"""))
      .withColumn(col, F.expr(s"""filter($col, x -> x != "NA")"""))
      .withColumn(col, F.expr(s"""filter($col, x -> x != "")"""))

    val groupped = splitted
      .flatMap { row =>
        val year = row.getAs[String]("year")
        row
          .getSeq[String](7)
          .combinations(2)
          .toSeq
          .map { arr =>
            arr.head -> arr.last
          }
          .groupBy(identity)
          .map { case (key, value) => (key, year, value.size) }

      }
      .withColumnRenamed("_1", "pairs")
      .withColumnRenamed("_2", "year")
      .withColumn("src", $"pairs".getItem("_1"))
      .withColumn("dst", $"pairs".getItem("_2"))

    val cleanSrc = cleanup(groupped, spark, "src")
    val cleanDst = cleanup(cleanSrc, spark, "dst")

    cleanDst
      .groupBy("src", "dst", "year")
      .sum("_3")
      .withColumnRenamed("sum(_3)", "count")
      .groupBy("src", "dst")
      .pivot("year")
      .sum("count")
      .withColumn("2017", F.coalesce($"2017", F.lit(0)))
      .withColumn("2018", F.coalesce($"2018", F.lit(0)))
      .withColumn("2019", F.coalesce($"2019", F.lit(0)))
      .withColumn("total", F.array("2017", "2018", "2019"))
      .withColumn("distance", F.expr("aggregate(total, 0L, (acc, value) -> acc + value, acc -> 100000 / acc)"))
      .sort($"src", $"dst", $"distance")
  }

  def cleanup(df: DataFrame, sparkSession: SparkSession, col: String) = {

    df.withColumn(
      col,
      F.when(F.col(col).equalTo("Bash/Shell"), "Bash/Shell/PowerShell")
        .when(F.col(col).equalTo("HTML"), "HTML/CSS")
        .when(F.col(col).equalTo("Google Cloud Platform"), "Google Cloud Platform/App Engine")
        .when(F.col(col).equalTo("Linux Desktop"), "Linux")
        .when(F.col(col).equalTo("Mac OS"), "MacOS")
        .when(F.col(col).equalTo("Windows Desktop or Server"), "Windows")
        .when(F.col(col).equalTo("Windows Desktop"), "Windows")
        .when(F.col(col).equalTo("BSD"), "BSD/Unix")
        .when(F.col(col).equalTo("Angular"), "Angular/Angular.js")
        .otherwise(F.col(col))
    )
  }
  def run(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CSV to Dataset")
      .config("spark.driver.host", "localhost")
      .master("local[*]")
      .getOrCreate

    val csvReader = spark.read.format("csv").option("header", "true")

    val in2019: DataFrame = csvReader
      .load(s"src/main/resources/2019.csv")
      .select("LanguageWorkedWith", "DatabaseWorkedWith", "PlatformWorkedWith", "WebFrameWorkedWith", "MiscTechWorkedWith", "DevEnviron", "OpSys")
      .withColumnRenamed("LanguageWorkedWith", "language")
      .withColumnRenamed("DatabaseWorkedWith", "database")
      .withColumnRenamed("PlatformWorkedWith", "platform")
      .withColumnRenamed("DevEnviron", "ide")
      .withColumnRenamed("OpSys", "os")
      .withColumn("framework", F.concat_ws(";", F.col("WebFrameWorkedWith"), F.col("MiscTechWorkedWith")))
      .drop("WebFrameWorkedWith")
      .drop("MiscTechWorkedWith")
      .withColumn("year", F.lit("2019"))
      .cache()

    val in2018 = csvReader
      .load(s"src/main/resources/2018.csv")
      .select("LanguageWorkedWith", "DatabaseWorkedWith", "PlatformWorkedWith", "FrameworkWorkedWith", "IDE", "OperatingSystem")
      .withColumnRenamed("LanguageWorkedWith", "language")
      .withColumnRenamed("DatabaseWorkedWith", "database")
      .withColumnRenamed("PlatformWorkedWith", "platform")
      .withColumnRenamed("IDE", "ide")
      .withColumnRenamed("OperatingSystem", "os")
      .withColumnRenamed("FrameworkWorkedWith", "framework")
      .withColumn("year", F.lit("2018"))
      .cache()

    val in2017 = csvReader
      .option("quote", "\"") // ,"With a hard ""g,"" like ""gift"""
      .option("escape", "\"")
      .load(s"src/main/resources/2017.csv")
      .select("HaveWorkedLanguage", "HaveWorkedDatabase", "HaveWorkedPlatform", "HaveWorkedFramework", "IDE")
      .withColumnRenamed("HaveWorkedLanguage", "language")
      .withColumnRenamed("HaveWorkedDatabase", "database")
      .withColumnRenamed("HaveWorkedPlatform", "platform")
      .withColumnRenamed("IDE", "ide")
      .withColumnRenamed("HaveWorkedFramework", "framework")
      .withColumn("os", F.lit("NA"))
      .withColumn("year", F.lit("2017"))
      .cache()

    val fields = Seq("language", "database", "platform", "ide", "framework", "os")

    val in = in2019.union(in2018).union(in2017).cache()

    val counts = fields.map(explodeAndCount(in))

    val dirty      = counts.reduce(_ union _)
    val cleanNodes = cleanup(dirty, spark, "language")
    val result = cleanNodes
      .groupBy("language", "type")
      .pivot("year")
      .max("count")
      .sort("type", "language")
      .withColumnRenamed("language", "id")

    // result.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").csv("nodes.csv") // format("json")

    val combs = combinations(spark, in.withColumn("combinations", F.lit("")))(fields).coalesce(1)

    combs.show(100, false)
    result.show(100, false)

    // combs.write.format("csv").option("header", "true").mode("overwrite").csv("edges.csv") // format("json")

  }

  run()

}
