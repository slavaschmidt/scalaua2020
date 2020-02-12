package com.slasch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.graphframes.GraphFrame
import org.apache.spark.sql.{functions => F, _}

object SparkGraph extends App {

  lazy val spark = SparkSession
    .builder()
    .config("spark.driver.host", "localhost")
    .appName("ScalaUA2020")
    .master("local[*]")
    .getOrCreate()

  def create_graph() = {

    val nodeSchema = new StructType()
      .add("id", StringType)
      .add("type", StringType)
      .add("2017", IntegerType)
      .add("2018", IntegerType)
      .add("2019", IntegerType)

    val edgeSchema = new StructType()
      .add("src", StringType)
      .add("dst", StringType)
      .add("2017", IntegerType)
      .add("2018", IntegerType)
      .add("2019", IntegerType)

    /*
    WRONG - edge overwrites node settings

    val csvReader = spark.read.format("csv").option("header", "true")

    val nodeReader = csvReader.schema(nodeSchema)
    val edgeReader = csvReader.schema(edgeSchema)
     */

    val nodeReader = spark.read.format("csv").option("header", "true").schema(nodeSchema)
    val edgeReader = spark.read.format("csv").option("header", "true").schema(edgeSchema)

    val vertices    = nodeReader.load("src/main/resources/vertices.csv")
    val directEdges = edgeReader.load("src/main/resources/edges.csv").cache()

    val reverseEdges = directEdges
      .withColumnRenamed("src", "_src")
      .withColumnRenamed("dst", "src")
      .withColumnRenamed("_src", "dst")

    val edges = directEdges.union(reverseEdges)

    GraphFrame(vertices, edges)
  }

  val graph = create_graph()

  graph.vertices
    .withColumn("total", F.array("2017", "2018", "2019"))
    .withColumn("total", F.expr("aggregate(total, 0L, (acc, value) -> acc + value)"))
    .orderBy(F.col("total").desc_nulls_last)
    .show(5)

  graph.edges
    .withColumn("total", F.array("2017", "2018", "2019"))
    .withColumn("total", F.expr("aggregate(total, 0L, (acc, value) -> acc + value)"))
    .orderBy(F.col("total").asc_nulls_last)
    .show(5)

  // java.lang.NoSuchMethodError: scala.Predef$.refArrayOps([Ljava/lang/Object;)Lscala/collection/mutable/ArrayOps;
  // change scala from 2.12 to 2.11
  graph.bfs
    .fromExpr("id='Scala'")
    .toExpr("id='Sublime Text' and type='ide'")
    .edgeFilter("src != 'Scala' or dst != 'Sublime Text'")
    .run()
    .show(false)

}
