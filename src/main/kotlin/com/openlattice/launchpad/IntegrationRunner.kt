package com.openlattice.launchpad

import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.Multimaps
import com.openlattice.launchpad.configuration.Integration
import com.openlattice.launchpad.configuration.IntegrationConfiguration
import com.openlattice.launchpad.postgres.BasePostgresIterable
import com.openlattice.launchpad.postgres.StatementHolderSupplier
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 *
 */
@SuppressFBWarnings(value = ["BC"], justification = "No cast found")
class IntegrationRunner {
    companion object {
        private val logger = LoggerFactory.getLogger(IntegrationRunner::class.java)

        private lateinit var launchLogger: LaunchpadLogger

        @SuppressFBWarnings(
                value = ["DM_EXIT"],
                justification = "Intentionally shutting down JVM on terminal error"
        )
        @VisibleForTesting
        @JvmStatic
        fun runIntegrations(
                integrationConfiguration: IntegrationConfiguration,
                session: SparkSession
        ): Map<String, Map<String, List<String>>> {
            // map to lakes if needed. This should be removed once launchpads are upgraded
            val lakes = integrationConfiguration.convertToDataLakesIfPresent()

            try {
                launchLogger = LaunchpadLogger.createLogger(lakes)
            } catch ( ex: Exception ) {
                logger.error("Unable to create launchpad logger. " +
                        "The likeliest possibilities are the connection timed out due to a firewall rule " +
                        "or there is an error in the config file for the datalake with launchpadLogger set to true", ex)
            }

            return integrationConfiguration.integrations.map { ( sourceLakeName, destToIntegration )->
                val value = Multimaps.asMap(destToIntegration).map { ( destinationLakeName, integrations ) ->
                    val destination = lakes.getValue(destinationLakeName)
                    val extIntegrations = integrations.filter { !it.gluttony } +
                            integrations.filter {
                                it.gluttony
                            }.flatMap { integration ->
                                // Gluttony query generation
                                BasePostgresIterable(
                                        StatementHolderSupplier(destination.getHikariDatasource(), integration.source)
                                ) { rs ->
                                    //TODO: Add support for reading gluttony flag, master sql, upsert sql.
                                    Integration(
                                            rs.getString("description"),
                                            rs.getString("query"),
                                            rs.getString("destination")
                                    )
                                }
                            }

                    val sourceLake = lakes.getValue(sourceLakeName)
                    val paths = AbstractRunner.writeUsingSpark(
                            integrationConfiguration,
                            sourceLake,
                            destination,
                            extIntegrations,
                            session,
                            launchLogger
                    )
                    destinationLakeName to paths
                }.toMap()
                sourceLakeName to value
            }.toMap()
        }
    }
}