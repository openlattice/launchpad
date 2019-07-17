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
import com.openlattice.launchpad.postgres.BasePostgresIterable
import com.openlattice.launchpad.postgres.StatementHolderSupplier
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.net.InetAddress
import java.time.OffsetDateTime
import java.util.*

/**
 *
 */
@SuppressFBWarnings(value = ["BC"], justification = "No cast found")
class IntegrationRunner {
    companion object {
        private val logger = LoggerFactory.getLogger(IntegrationRunner::class.java)
        private val sparkSession = SparkSession.builder()
                .master("local[${Runtime.getRuntime().availableProcessors()}]")
                .appName("integration")
                .orCreate
        private val hostName = try {
            val localhost = InetAddress.getLocalHost()
            if (localhost.hostName.isBlank()) {
                localhost.hostAddress
            } else {
                localhost.hostName
            }
        } catch (ex: Exception) {
            val id = UUID.randomUUID()
            logger.warn("Unable to get host for this machine. Using to random id: $id")
            id.toString()
        }

        @SuppressFBWarnings(
                value = ["DM_EXIT"],
                justification = "Intentionally shutting down JVM on terminal error"
        )
        @VisibleForTesting
        @JvmStatic
        fun runIntegrations(integrationConfiguration: IntegrationConfiguration) {
            val integrationsMap = integrationConfiguration.integrations
            val datasources = integrationConfiguration.datasources.map { it.name to it }.toMap()
            val destinations = integrationConfiguration.destinations.map { it.name to it }.toMap()

            destinations.forEach { (_, destination) ->
                destination.hikariDatasource.connection.use { conn ->
                    conn.createStatement().use { stmt ->
                        stmt.execute(IntegrationTables.CREATE_INTEGRATION_ACTIVITY_SQL)
                    }
                }
            }

            integrationsMap.forEach { (datasourceName, destinationsForDatasource) ->
                val datasource = datasources.getValue(datasourceName)

                Multimaps.asMap(destinationsForDatasource).forEach { (destinationName, integrations) ->
                    val extIntegrations = integrations.filter { !it.gluttony } + integrations
                            .filter { it.gluttony }
                            .flatMap { integration ->

                                val destination = destinations.getValue(destinationName)
                                BasePostgresIterable(
                                        StatementHolderSupplier(destination.hikariDatasource, integration.source)
                                ) { rs ->
                                    Integration(
                                            rs.getString("description"),
                                            rs.getString("query"),
                                            rs.getString("destination")
                                    )
                                }
                            }

                    extIntegrations.forEach { integration ->
                        val destination = destinations.getValue(destinationName)

                        logger.info("Running integration: {}", integration)
                        val start = OffsetDateTime.now()
                        logStarted(integrationConfiguration.name, destination, integration, start)
                        val ds = try {
                            logger.info("Transferring ${datasource.name} with query ${integration.source}")
                            getSourceDataset(datasource, integration)
                        } catch (ex: Exception) {
                            logFailed(datasourceName, destination, integration, start)
                            logger.error(
                                    "Integration {} failed going from {} to {}. Exiting.",
                                    integrationConfiguration.name,
                                    integration.source,
                                    integration.destination,
                                    ex
                            )

                            kotlin.system.exitProcess(1)
                        }
                        logger.info("Read from source: {}", datasource)
                        //Only CSV and JDBC are tested.
                        when (destination.writeDriver) {
                            CSV_DRIVER -> ds.write().option(HEADER, true).csv(destination.writeUrl)
                            PARQUET_DRIVER -> ds.write().parquet(destination.writeUrl)
                            ORC_DRIVER -> ds.write().orc(destination.writeUrl)
                            else -> toDatabase(integrationConfiguration.name, ds, destination, integration, start)
                        }
                        logger.info("Wrote to name: {}", destination)
                    }
                }
            }
        }

        @SuppressFBWarnings(
                value = ["DM_EXIT"],
                justification = "Intentionally shutting down JVM on terminal error"
        )
        @JvmStatic
        private fun toDatabase(
                integrationName: String,
                ds: Dataset<Row>,
                destination: LaunchpadDestination,
                integration: Integration,
                start: OffsetDateTime
        ) {
            try {
                ds.write()
                        .option("batchsize", destination.batchSize.toLong())
                        .option(WRITE_DRIVER, destination.writeDriver)
                        .mode(SaveMode.Overwrite)
                        .jdbc(
                                destination.writeUrl,
                                integration.destination,
                                destination.properties
                        )
                logSuccessful(integrationName, destination, integration, start)
            } catch (ex: Exception) {
                logFailed(integrationName, destination, integration, start)
                logger.error(
                        "Integration {} failed going from {} to {}. Exiting.",
                        integrationName,
                        integration.source,
                        integration.destination,
                        ex
                )

                kotlin.system.exitProcess(1)
            }
        }

        private fun logStarted(
                integrationName: String,
                destination: LaunchpadDestination,
                integration: Integration,
                start: OffsetDateTime
        ) {
            try {
                unsafeExecuteSql(
                        IntegrationTables.LOG_INTEGRATION_STARTED,
                        integrationName,
                        destination,
                        integration,
                        start
                )
            } catch (ex: Exception) {
                logger.warn("Unable to create activity entry in the database. Continuing data transfer...", ex)
            }
        }

        @SuppressFBWarnings(value = ["OBL_UNSATISFIED_OBLIGATION"], justification = "Spotbugs doens't like kotlin")
        private fun unsafeExecuteSql(
                sql: String,
                integrationName: String,
                destination: LaunchpadDestination,
                integration: Integration,
                start: OffsetDateTime
        ) {
            destination.hikariDatasource.connection.use { connection ->
                connection.prepareStatement(sql).use { ps ->
                    ps.setString(1, integrationName)
                    ps.setString(2, hostName)
                    ps.setString(3, integration.destination)
                    ps.setObject(4, start)
                    ps.executeUpdate()
                }
            }
        }

        private fun logSuccessful(
                integrationName: String,
                destination: LaunchpadDestination,
                integration: Integration,
                start: OffsetDateTime
        ) {
            try {
                unsafeExecuteSql(
                        IntegrationTables.LOG_SUCCESSFUL_INTEGRATION,
                        integrationName,
                        destination,
                        integration,
                        start
                )
            } catch (ex: Exception) {
                logger.warn("Unable to log success to database. Continuing data transfer...", ex)
            }
        }

        private fun logFailed(
                integrationName: String,
                destination: LaunchpadDestination,
                integration: Integration,
                start: OffsetDateTime
        ) {
            try {
                unsafeExecuteSql(
                        IntegrationTables.LOG_FAILED_INTEGRATION,
                        integrationName,
                        destination,
                        integration,
                        start
                )
            } catch (ex: Exception) {
                logger.warn("Unable to log failure to database. Terminating", ex)
            }
        }

        @JvmStatic
        private fun getSourceDataset(datasource: LaunchpadDatasource, integration: Integration): Dataset<Row> {
            when (datasource.driver) {
                CSV_DRIVER -> return sparkSession
                        .read()
                        .option(HEADER, datasource.header)
                        .option("inferSchema", true)
                        .csv(datasource.url + integration.source)
                else -> return sparkSession
                        .read()
                        .format("jdbc")
                        .option(URL, datasource.url)
                        .option("dbtable", integration.source)
                        .option("user", datasource.user)
                        .option(PASSWORD, datasource.password)
                        .option(DRIVER, datasource.driver)
                        .option("fetchSize", datasource.fetchSize.toLong())
                        .load()
            }
        }


    }
}
