package com.openlattice.launchpad

import com.openlattice.launchpad.configuration.IntegrationConfiguration
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class ArchiveRunner {
    companion object {

        private val logger = LoggerFactory.getLogger(ArchiveRunner::class.java)

        private lateinit var launchLogger: LaunchpadLogger

        @JvmStatic
        fun runArchives(
                integrationConfiguration: IntegrationConfiguration,
                session: SparkSession
        ) {
            val dataLakesByName = integrationConfiguration.datalakes.get().associateBy { it.name }
            try {
                launchLogger = LaunchpadLogger.createLogger( dataLakesByName )
            } catch ( ex: Exception ) {
                logger.error("Unable to create launchpad logger. " +
                        "The likeliest possibilities are the connection timed out due to a firewall rule " +
                        "or there is an error in the config file for the datalake with launchpadLogger set to true", ex)
            }

            integrationConfiguration.archives.map { (source, destinationToArchive) ->
                val sourceLake = dataLakesByName[source]
                destinationToArchive.map { ( destinationLake, archives ) ->
                    val destLake = dataLakesByName[destinationLake]
                    archives.forEach {
                        val query = it.getArchiveSql()
                    }
                }
            }
        }
    }

    val dateQuery = """
            Select * from <table> where <constraints> AND <dateCol> >= <startDate> AND <dateCol> < <startDate> + <num> <interval>
        """.trimIndent()

}