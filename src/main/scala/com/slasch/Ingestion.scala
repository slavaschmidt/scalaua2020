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
      .withColumn(col, F.split(F.col(col), "\\s*;\\s*"))
      .withColumn(col, F.expr(s"""filter($col, x -> x != "N/A" && x != "Other(s):")"""))
      .filter(_.getAs[String](col).nonEmpty)


      splitted.flatMap { row =>
        val year = row.getAs[String]("year")
        row.getSeq[String](7)
          .combinations(2)
          .toSeq
          .map { arr => arr.head -> arr.last }
          .groupBy(identity)
          .map { case (key, value) => (key, year, value.size) }

      }.groupBy("_1", "_2").sum("_3")
  }

  def run(): Unit = {
    val spark = SparkSession.builder()
     .appName("CSV to Dataset")
     .config("spark.driver.host", "localhost")
     .master("local")
     .getOrCreate

    val csvReader = spark.read.format("csv").option("header", "true")

    val in2019: DataFrame =  csvReader
      .load(s"src/main/resources/2019.csv")
      .select("LanguageWorkedWith","DatabaseWorkedWith","PlatformWorkedWith","WebFrameWorkedWith","MiscTechWorkedWith","DevEnviron","OpSys")
      .withColumnRenamed("LanguageWorkedWith", "language")
      .withColumnRenamed("DatabaseWorkedWith", "database")
      .withColumnRenamed("PlatformWorkedWith", "platform")
      .withColumnRenamed("DevEnviron", "ide")
      .withColumnRenamed("OpSys", "os")
      .withColumn("framework", F.concat_ws(";",F.col("WebFrameWorkedWith"), F.col("MiscTechWorkedWith")))
      .drop("WebFrameWorkedWith")
      .drop("MiscTechWorkedWith")
      .withColumn("year", F.lit("2019"))
      .cache()

    val in2018 =  csvReader
      .load(s"src/main/resources/2018.csv")
      .select("LanguageWorkedWith","DatabaseWorkedWith","PlatformWorkedWith","FrameworkWorkedWith","IDE","OperatingSystem")
      .withColumnRenamed("LanguageWorkedWith", "language")
      .withColumnRenamed("DatabaseWorkedWith", "database")
      .withColumnRenamed("PlatformWorkedWith", "platform")
      .withColumnRenamed("IDE", "ide")
      .withColumnRenamed("OperatingSystem", "os")
      .withColumnRenamed("FrameworkWorkedWith", "framework")
      .withColumn("year", F.lit("2018"))
      .cache()

    val in2017 =  csvReader
      .option("quote", "\"") // ,"With a hard ""g,"" like ""gift"""
      .option("escape", "\"")
      .load(s"src/main/resources/2017.csv")
      .select("HaveWorkedLanguage","HaveWorkedDatabase","HaveWorkedPlatform","HaveWorkedFramework","IDE")
      .withColumnRenamed("HaveWorkedLanguage", "language")
      .withColumnRenamed("HaveWorkedDatabase", "database")
      .withColumnRenamed("HaveWorkedPlatform", "platform")
      .withColumnRenamed("IDE", "ide")
      .withColumnRenamed("HaveWorkedFramework", "framework")
      .withColumn("os", F.lit("NA"))
      .withColumn("year", F.lit("2017"))
      .cache()

    val fields = Seq("language", "database", "platform", "ide", "framework", "os")

    val in = in2019.union(in2018).union(in2017)

    val counts = fields.map(explodeAndCount(in))

    val result = counts.reduce(_ union _)
      .groupBy("language", "type").pivot("year").max("count")
      .sort("type", "language")
      .withColumnRenamed("language", "id")

    // result.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").csv("counts.csv") // format("json")

    result.show(1000, false)

    // val combs = combinations(spark, in.withColumn("combinations", F.lit("")))(fields)

    // combs.show(100, false)
  }

  run()

}
