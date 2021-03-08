package org.neo4j.spark

import org.apache.spark.SparkException
import org.junit.Assert.{assertTrue, fail}
import org.junit.{Assume, BeforeClass, Test}
import org.neo4j.driver.exceptions.ClientException

object DataSourceReaderNeo4j35xTSE {
  @BeforeClass
  def checkNeo4jVersion() {
    Assume.assumeTrue(TestUtil.neo4jVersion().startsWith("3.5"))
  }
}

class DataSourceReaderNeo4j35xTSE extends SparkConnectorScalaBaseTSE {
  @Test
  def testShouldThrowClearErrorIfADbIsSpecified(): Unit = {
    try {
      ss.read.format(classOf[DataSource].getName)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("database", "db1")
        .option("labels", "MATCH (h:Household) RETURN id(h)")
        .load()
        .show()
    }
    catch {
      case clientException: ClientException => {
        assertTrue(clientException.getMessage.equals(
          "Database name parameter for selecting database is not supported in Bolt Protocol Version 3.0. Database name: 'db1'"
        ))
      }
      case generic: Throwable => fail(s"should be thrown a ${classOf[SparkException].getName}, got ${generic.getClass} instead")
    }
  }
}
