package com.openlattice.launchpad

import org.apache.spark.sql.SparkSession

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
data class SparkSessionShutdownHook( val session: SparkSession ) : Thread() {
    override fun run() {
        session.sparkContext().stop()
    }
}
