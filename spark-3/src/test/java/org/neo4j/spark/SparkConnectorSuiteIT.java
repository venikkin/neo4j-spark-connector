package org.neo4j.spark;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    DataSourceReaderTypesTSE.class
})
public class SparkConnectorSuiteIT extends SparkConnectorScalaSuiteIT {
}
