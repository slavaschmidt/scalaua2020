package com.slasch

import java.io.File

import scala.collection.JavaConverters._
import org.neo4j.driver.v1.{AuthTokens, Config, GraphDatabase, Session, StatementResult}

object Neo4jConnection extends App {

  val path = "file:///src/main/resources/"
  val scriptNodes =
    s"""
       |WITH "$path" AS base
       |WITH base + "vertices.csv" AS path
       |LOAD CSV WITH HEADERS FROM path AS row
       |MERGE (tech:Technology {id:row.id})
       |SET
       |  tech.year2017 = toInteger(row['2017']), tech.year2018 = toInteger(row['2018']), tech.year2019 = toInteger(row['2019']),
       |  tech.total = tech.year2017 + tech.year2018 + tech.year2019,
       |  tech.type = row.type
       |""".stripMargin

  val scriptEdges =
    s"""
       |WITH "$path" AS base
       |WITH base + "edges.csv" AS path
       |LOAD CSV WITH HEADERS FROM path AS row
       |MATCH (l:Technology {id: row.src})
       |MATCH (r:Place {id: row.dst})
       |MERGE (l)-[:EROAD {
       | distance: toFloat(row.distance),
       | year2017: toInteger(row['2017']),
       | year2018: toInteger(row['2018']),
       | year2019: toInteger(row['2019'])
       |}]->(r)
       |""".stripMargin

  def ingest() = withDriver { session =>
    val ingestNodes: StatementResult = session.run(scriptNodes)
    println(ingestNodes.summary())

    val ingestEdges: StatementResult = session.run(scriptEdges)
    println(ingestEdges.summary())
  }

  def withDriver[T](block: Session => T) = {
    val driver = GraphDatabase.driver("bolt://localhost/7687", AuthTokens.basic("neo4j", "scalaua"))
    val session = driver.session()
    try {
      block(session)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      session.close()
      driver.close()
    }
  }


  // ingest()

  def sp = {
    val statement =
      s"""
         |MATCH (source:Technology {id: "Scala"}), (destination:Technology {id: "JavaScript"})
         |CALL algo.shortestPath.stream(source, destination, null) YIELD nodeId, cost
         |RETURN algo.getNodeById(nodeId).id AS place, cost
         |""".stripMargin

  }

}
