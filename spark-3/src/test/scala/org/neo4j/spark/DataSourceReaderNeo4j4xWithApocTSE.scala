package org.neo4j.spark

import org.junit.Assert.assertEquals
import org.junit.{Assume, BeforeClass, Test}
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{SessionConfig, Transaction, TransactionWork}

object DataSourceReaderNeo4j4xWithApocTSE {
  @BeforeClass
  def checkNeo4jVersion() {
    Assume.assumeFalse(TestUtil.neo4jVersion().startsWith("3.5"))
  }
}

class DataSourceReaderNeo4j4xWithApocTSE extends SparkConnectorScalaBaseWithApocTSE {

  @Test
  def testMultiDbJoin(): Unit = {
    SparkConnectorScalaSuiteWithApocIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(
            """
      CREATE (p1:Person:Customer {name: 'John Doe'}),
       (p2:Person:Customer {name: 'Mark Brown'}),
       (p3:Person:Customer {name: 'Cindy White'})
      """).consume()
        })

    SparkConnectorScalaSuiteWithApocIT.driver.session(SessionConfig.forDatabase("db2"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(
            """
      CREATE (p1:Person:Employee {name: 'Jane Doe'}),
       (p2:Person:Employee {name: 'John Doe'})
      """).consume()
        })

    val df1 = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("database", "db1")
      .option("labels", "Person")
      .load()

    val df2 = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("database", "db2")
      .option("labels", "Person")
      .load()

    assertEquals(3, df1.count())
    assertEquals(2, df2.count())

    val dfJoin = df1.join(df2, df1("name") === df2("name"))
    assertEquals(1, dfJoin.count())
  }

}
