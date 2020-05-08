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

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.Multimaps
import com.openlattice.launchpad.configuration.Constants.CSV_FORMAT
import com.openlattice.launchpad.configuration.Constants.FILESYSTEM_DRIVER
import com.openlattice.launchpad.configuration.Constants.LEGACY_CSV_FORMAT
import com.openlattice.launchpad.configuration.Constants.ORC_FORMAT
import com.openlattice.launchpad.configuration.Constants.S3_DRIVER
import com.openlattice.launchpad.postgres.BasePostgresIterable
import com.openlattice.launchpad.postgres.StatementHolderSupplier
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.time.Clock
import java.time.OffsetDateTime
import java.util.*

/**
 *
 */
@SuppressFBWarnings(value = ["BC"], justification = "No cast found")
class IntegrationRunner {
    companion object {
        private val logger = LoggerFactory.getLogger(IntegrationRunner::class.java)

        private val timer = MetricRegistry().timer("uploadTime")

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

        fun configureOrGetSparkSession(integrationConfiguration: IntegrationConfiguration): SparkSession {
            val session = SparkSession.builder()
                    .master("local[${Runtime.getRuntime().availableProcessors()}]")
                    .appName("integration")
            if ( integrationConfiguration.awsConfig.isPresent ){
                val config = integrationConfiguration.awsConfig.get()
                session
                        .config("fs.s3a.access.key", config.accessKeyId )
                        .config("fs.s3a.secret.key", config.secretAccessKey )
                        .config("fs.s3a.endpoint", "s3.${config.regionName}.amazonaws.com")
                        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")
                        .config("spark.hadoop.fs.s3a.fast.upload","true")
                        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                        .config("spark.speculation", "false")
            }
            return session.orCreate
        }

        @SuppressFBWarnings(
                value = ["DM_EXIT"],
                justification = "Intentionally shutting down JVM on terminal error"
        )
        @VisibleForTesting
        @JvmStatic
        fun runIntegrations(integrationConfiguration: IntegrationConfiguration) {
            val integrationsMap = integrationConfiguration.integrations
            val sourcesConfig = integrationConfiguration.datasources.orElse(listOf())
            val destsConfig = integrationConfiguration.destinations.orElse(listOf())

            // map to lakes if needed. This will be removed once launchpads are upgraded
            val lakes = if ( integrationConfiguration.datalakes.isEmpty ){
                val newLakes = ArrayList<DataLake>()
                destsConfig.forEach { newLakes.add(it.asDataLake()) }
                sourcesConfig.forEach { newLakes.add(it.asDataLake()) }
                newLakes
            } else {
                integrationConfiguration.datalakes.get()
            }.map{ it.name to it }.toMap()

            lakes.filter {
                isLatticeLoggingLake( it.value )
            }.forEach { (_, destination) ->
                destination.getHikariDatasource().connection.use { conn ->
                    conn.createStatement().use { stmt ->
                        stmt.execute(IntegrationTables.CREATE_INTEGRATION_ACTIVITY_SQL)
                    }
                }
            }

            val session = configureOrGetSparkSession(integrationConfiguration)

            integrationsMap.forEach { sourceLakeName, destToIntegration ->
                val sourceLake = lakes.getValue( sourceLakeName )
                Multimaps.asMap(destToIntegration).forEach { destinationName, integrations ->
                    val extIntegrations = integrations.filter { !it.gluttony } + integrations
                            .filter { it.gluttony }
                            .flatMap { integration ->

                                val destLake = lakes.getValue(destinationName)
                                BasePostgresIterable(
                                        StatementHolderSupplier(destLake.getHikariDatasource(), integration.source)
                                ) { rs ->
                                    Integration(
                                            rs.getString("description"),
                                            rs.getString("query"),
                                            rs.getString("destination")
                                    )
                                }
                            }

                    extIntegrations.forEach { integration ->
                        val destination = lakes.getValue(destinationName)

                        logger.info("Running integration: {}", integration)
                        val start = OffsetDateTime.now()
                        logStarted(integrationConfiguration.name, destination, integration, start)
                        val ds = try {
                            logger.info("Transferring ${sourceLake.name} with query ${integration.source}")
                            getSourceDataset(sourceLake, integration, session)
                        } catch (ex: Exception) {
                            logFailed(sourceLakeName, destination, integration, start)
                            logger.error(
                                    "Integration {} failed going from {} to {}. Exiting.",
                                    integrationConfiguration.name,
                                    integration.source,
                                    integration.destination,
                                    ex
                            )

                            kotlin.system.exitProcess(1)
                        }
                        logger.info("Read from source: {}", sourceLake)
                        //Only CSV and JDBC are tested.

                        val sparkWriter = when (destination.dataFormat) {
                            CSV_FORMAT, LEGACY_CSV_FORMAT -> {
                                ds.write()
                                        .option("header", true)
                                        .format( CSV_FORMAT )
                            }
                            ORC_FORMAT -> ds.write().format( ORC_FORMAT )
                            else -> {
                                ds.write()
                                        .option("batchsize", destination.batchSize.toLong())
                                        .option("driver", destination.driver)
                                        .mode( destination.writeMode )
                                        .format("jdbc")
                            }
                        }
                        logger.info("Created spark writer")
                        val ctxt = timer.time()
                        when (destination.driver) {
                            FILESYSTEM_DRIVER -> sparkWriter.save(destination.url)
                            S3_DRIVER -> {
                                sparkWriter.save("${destination.url}-${OffsetDateTime.now(Clock.systemUTC())}")
                            }
                            else -> toDatabase(integrationConfiguration.name, sparkWriter, destination, integration, start)
                        }
                        val elapsedNs = ctxt.stop()
                        val secs = elapsedNs/1_000_000_000.0
                        val mins = secs/60.0
                        logger.info("Finished writing to name: {} in {} seconds ({} minutes)", destination, secs, mins)
                    }
                }
            }
        }

        private fun isLatticeLoggingLake(destination: DataLake): Boolean {
            return destination.latticeLogger
        }

        @SuppressFBWarnings(
                value = ["DM_EXIT"],
                justification = "Intentionally shutting down JVM on terminal error"
        )
        @JvmStatic
        private fun toDatabase(
                integrationName: String,
                ds: DataFrameWriter<Row>,
                destination: DataLake,
                integration: Integration,
                start: OffsetDateTime
        ) {
            try {
                ds.jdbc(
                        destination.url,
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
                destination: DataLake,
                integration: Integration,
                start: OffsetDateTime
        ) {
            if (!isLatticeLoggingLake( destination )){
                logger.info("Starting integration $integrationName to ${destination.name}")
                return
            }
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

        @SuppressFBWarnings(value = ["OBL_UNSATISFIED_OBLIGATION"], justification = "Spotbugs doesn't like kotlin")
        private fun unsafeExecuteSql(
                sql: String,
                integrationName: String,
                destination: DataLake,
                integration: Integration,
                start: OffsetDateTime
        ) {
            destination.getHikariDatasource().connection.use { connection ->
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
                destination: DataLake,
                integration: Integration,
                start: OffsetDateTime
        ) {
            if (!isLatticeLoggingLake( destination )){
                logger.info("Integration succeeded")
                return
            }
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
                destination: DataLake,
                integration: Integration,
                start: OffsetDateTime
        ) {
            if (!isLatticeLoggingLake( destination )){
                logger.info("Integration failed")
                return
            }
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
        private fun getSourceDataset(datasource: DataLake, integration: Integration, sparkSession: SparkSession ): Dataset<Row> {
            when (datasource.driver) {
                CSV_FORMAT, LEGACY_CSV_FORMAT  -> return sparkSession
                        .read()
                        .option("header", datasource.header)
                        .option("inferSchema", true)
                        .csv(datasource.url + integration.source)
                else -> return sparkSession
                        .read()
                        .format("jdbc")
                        .option("url", datasource.url)
                        .option("dbtable", integration.source)
                        .option("user", datasource.user)
                        .option("password", datasource.password)
                        .option("driver", datasource.driver)
                        .option("fetchSize", datasource.fetchSize.toLong())
                        .load()
            }
        }
    }
}
