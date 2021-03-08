package org.neo4j.spark

import org.junit.Assert.assertEquals
import org.junit.{Assume, BeforeClass, Test}
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{Transaction, TransactionWork}

object DataSourceReaderNeo4j41xTSE {
  @BeforeClass
  def checkNeo4jVersion() {
    val neo4jVersion = TestUtil.neo4jVersion()
    Assume.assumeTrue(!neo4jVersion.startsWith("3.5") && !neo4jVersion.startsWith("4.0"))
  }
}

class DataSourceReaderNeo4j41xTSE extends SparkConnectorScalaBaseTSE {

  @Test
  def testEmptyDataset(): Unit = {
    val df = ss.read
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query", "MATCH (e:ID_DO_NOT_EXIST) RETURN id(e) as f, 1 as g")
      .load

    assertEquals(0, df.count())
    assertEquals(Seq("f", "g"), df.columns.toSeq)
  }

  @Test
  def testColumnSorted(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("CREATE (i1:Instrument{name: 'Drums', id: 1}), (i2:Instrument{name: 'Guitar', id: 2})").consume()
        })

    val df = ss.read
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query", "MATCH (i:Instrument) RETURN id(i) as internal_id, i.id as id, i.name as name, i.name")
      .load
      .orderBy("id")

    assertEquals(1L, df.collectAsList().get(0).get(1))
    assertEquals("Drums", df.collectAsList().get(0).get(2))
    assertEquals(Seq("internal_id", "id", "name", "i.name"), df.columns.toSeq)
  }

  @Test
  def testComplexReturnStatement(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query",
        """MATCH (p:Person)-[b:BOUGHT]->(pr:Product)
          |RETURN id(p) AS personId, id(pr) AS productId, {quantity: b.quantity, when: b.when} AS map, "some string" as someString, {anotherField: "201"} as map2""".stripMargin)
      .option("schema.strategy", "string")
      .load()

    assertEquals(Seq("personId", "productId", "map", "someString", "map2"), df.columns.toSeq)
    assertEquals(100, df.count())
  }

  @Test
  def testComplexReturnStatementNoValues(): Unit = {
    val df = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query",
        """MATCH (p:Person)-[b:BOUGHT]->(pr:Product)
          |RETURN id(p) AS personId, id(pr) AS productId, {quantity: b.quantity, when: b.when} AS map, "some string" as someString, {anotherField: "201", and: 1} as map2""".stripMargin)
      .option("schema.strategy", "string")
      .load()

    assertEquals(Seq("personId", "productId", "map", "someString", "map2"), df.columns.toSeq)
    assertEquals(0, df.count())
  }

}
