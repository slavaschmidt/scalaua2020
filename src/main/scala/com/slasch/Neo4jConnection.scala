package com.slasch

import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Session, StatementResult}

object Neo4jConnection extends App {

  val path = "file:///src/main/resources/"
  val constraintSql = "CREATE CONSTRAINT ON (t:Technology) ASSERT t.label IS UNIQUE" // also creates an index

  val scriptNodes =
    s"""
       |WITH "$path" AS base
       |WITH base + "vertices.csv" AS path
       |LOAD CSV WITH HEADERS FROM path AS row
       |MERGE (tech:Technology {label:row.name})
       |SET
       |  tech.year2017 = toInteger(row['2017']), tech.year2018 = toInteger(row['2018']), tech.year2019 = toInteger(row['2019']),
       |  tech.total = tech.year2017 + tech.year2018 + tech.year2019,
       |  tech.kind = row.type
       |""".stripMargin

  val scriptEdges =
    s"""
       |WITH "$path" AS base
       |WITH base + "edges.csv" AS path
       |LOAD CSV WITH HEADERS FROM path AS row
       |MATCH (l:Technology {label: row.src})
       |MATCH (r:Technology {label: row.dst})
       |MERGE (l)-[:CONNECTED {
       | year2017: toInteger(row['2017']),
       | year2018: toInteger(row['2018']),
       | year2019: toInteger(row['2019']),
       | closeness2017: toFloat(row['2017']) / (CASE WHEN l.year2017 = 0 OR r.year2017 = 0 THEN 0.001 ELSE CASE WHEN l.year2017<r.year2017 AND l.year2017 > 0 THEN l.year2017 ELSE r.year2017 END END),
       | closeness2018: toFloat(row['2018']) / (CASE WHEN l.year2018 = 0 OR r.year2018 = 0 THEN 0.001 ELSE CASE WHEN l.year2018<r.year2018 AND l.year2018 > 0 THEN l.year2018 ELSE r.year2018 END END),
       | closeness2019: toFloat(row['2019']) / (CASE WHEN l.year2019 = 0 OR r.year2019 = 0 THEN 0.001 ELSE CASE WHEN l.year2019<r.year2019 AND l.year2019 > 0 THEN l.year2019 ELSE r.year2019 END END),
       | closeness: (toFloat(row['2017']) + toFloat(row['2018']) + toFloat(row['2019'])) /
       |  (CASE WHEN l.total<r.total AND l.total > 0 THEN l.total ELSE r.total END)
       | }]->(r)
       |""".stripMargin

  def ingest() = withDriver { session =>

    val schema: StatementResult = session.run(constraintSql)
    println(schema.summary())

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


  ingest()

  def sp = {
    val statement =
      s"""
         |MATCH (source:Technology {id: "Scala"}), (destination:Technology {id: "JavaScript"})
         |CALL algo.shortestPath.stream(source, destination, null) YIELD nodeId, cost
         |RETURN algo.getNodeById(nodeId).id AS place, cost
         |""".stripMargin

  }

}
