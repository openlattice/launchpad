/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.launchpad.configuration

import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.Multimaps
import com.openlattice.launchpad.AbstractRunner
import com.openlattice.launchpad.LaunchpadLogger
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
                launchLogger = LaunchpadLogger.createLogger( lakes )
            } catch ( ex: Exception ) {
                logger.error("Unable to create launchpad logger. " +
                        "The likeliest possibilities are the connection timed out due to a firewall rule " +
                        "or there is an error in the config file for the datalake with launchpadLogger set to true", ex)
            }

            return integrationConfiguration.integrations.map { ( sourceLakeName, destToIntegration )->
                val value = Multimaps.asMap(destToIntegration).map { ( destinationLakeName, integrations ) ->
                    val destination = lakes.getValue(destinationLakeName)
                    val extIntegrations = integrations.filter { !it.gluttony } +
                            integrations.filter { it.gluttony }.flatMap { integration ->
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
