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
        ): Map<String, Map<String, List<String>>> {
            val dataLakesByName = integrationConfiguration.datalakes.get().associateBy { it.name }
            try {
                launchLogger = LaunchpadLogger.createLogger(dataLakesByName)
            } catch (ex: Exception) {
                logger.error("Unable to create launchpad logger. " +
                        "The likeliest possibilities are the connection timed out due to a firewall rule " +
                        "or there is an error in the config file for the datalake with launchpadLogger set to true", ex)
            }

            return integrationConfiguration.archives.map { (source, destinationToArchive) ->
                val value = destinationToArchive.map { (destinationLake, archives) ->
                    val paths = AbstractRunner.writeUsingSpark(
                            integrationConfiguration,
                            dataLakesByName.getValue(source),
                            dataLakesByName.getValue(destinationLake),
                            archives,
                            session,
                            launchLogger
                    )
                    destinationLake to paths
                }.toMap()
                source to value
            }.toMap()
        }
    }
}