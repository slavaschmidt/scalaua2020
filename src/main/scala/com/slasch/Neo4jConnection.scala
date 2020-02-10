package com.slasch

import org.apache.spark.sql.SparkSession
import org.neo4j.spark.{Neo4j, Neo4jConfig, Neo4jDataFrame}

object Neo4jConnection extends App {

  val spark = SparkSession
    .builder()
    .config(Neo4jConfig.prefix + "url", "bolt://localhost:7687")
    .config(Neo4jConfig.prefix + "user", "neo4j")
    .config(Neo4jConfig.prefix + "password", "pass")
    .config("spark.driver.host", "localhost")
    .appName("ScalaUA2020")
    .master("local[*]")
    .getOrCreate()

  lazy val cypher = {
    val queryParam = Map[String, Object](
      "ass_id" -> "ass_id",
      "chas" -> "chas",
      "asse_id" -> "assem_id"
    )
    val query="MATCH (c:c {xyz:{xyz},chs:{chs}} ) MATCH (d:ass {ass_id:{ass_id}}) CREATE(c)-[w:ASS_IN]->(d) return w"

    val sc = spark.sparkContext
    Neo4j(sc).cypher(query,queryParam).loadDataFrame()

    val sqlContext = spark.sqlContext
    Neo4jDataFrame.apply(sqlContext,query,queryParam.toSeq)
  }


}
