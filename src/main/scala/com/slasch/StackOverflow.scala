
import org.apache.spark.sql.Dataset

import org.apache.spark.sql.{functions => F, _}

object StackOverflow extends App {

  def explodeAndCount(df: DataFrame)(col: String) = {
    df.withColumn(col, F.split(F.col(col), ";\\s?"))
      .withColumn(col, F.explode(F.col(col)))
      .withColumn("type", F.lit(col))
      .filter(_.getAs[String](col) != "NA")
      .groupBy(col, "type", "year")
      .count()
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
      .withColumn("framework", F.concat_ws("; s",F.col("WebFrameWorkedWith"), F.col("MiscTechWorkedWith")))
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

    val result: DataFrame = counts.reduce(_ union _)

    result.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").csv("counts.csv") // format("json")

    result.show(100)

  }

  run()

}
