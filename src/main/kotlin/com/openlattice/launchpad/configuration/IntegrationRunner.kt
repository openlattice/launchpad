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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.Multimaps
import com.openlattice.launchpad.LaunchpadLogger
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

        private lateinit var launchLogger: LaunchpadLogger

        fun configureOrGetSparkSession(integrationConfiguration: IntegrationConfiguration): SparkSession {
            val session = SparkSession.builder()
                    .master("local[${Runtime.getRuntime().availableProcessors()}]")
                    .appName("integration")
            if (integrationConfiguration.awsConfig.isPresent) {
                val config = DefaultAWSCredentialsProviderChain.getInstance().credentials
                val manualConfig = integrationConfiguration.awsConfig.get()
                session
                        .config("fs.s3a.access.key", config.awsAccessKeyId)
                        .config("fs.s3a.secret.key", config.awsSecretKey)
                        .config("fs.s3a.endpoint", "s3.${manualConfig.regionName}.amazonaws.com")
                        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
                        .config("spark.hadoop.fs.s3a.fast.upload", "true")
                        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                        .config("spark.speculation", "false")
            }
            return session.orCreate
        }

        @JvmStatic
        fun convertToDataLakesIfPresent(integrationConfiguration: IntegrationConfiguration): Map<String, DataLake> {
            val sourcesConfig = integrationConfiguration.datasources.orElse(listOf())
            val destsConfig = integrationConfiguration.destinations.orElse(listOf())

            // map to lakes if needed. This will be removed once launchpads are upgraded
            return integrationConfiguration.datalakes.orElseGet {
                val newLakes = ArrayList<DataLake>(destsConfig.size + sourcesConfig.size)
                destsConfig.forEach { newLakes.add(it.asDataLake()) }
                sourcesConfig.forEach { newLakes.add(it.asDataLake()) }
                newLakes
            }.associateBy { it.name }
        }

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
            val lakes = convertToDataLakesIfPresent(integrationConfiguration)

            val loggerLake = lakes.filter {
                it.value.latticeLogger
            }.entries.first().value

            launchLogger = LaunchpadLogger.fromLake( loggerLake )
            launchLogger.createLoggingTable()

            val integrationsMap = integrationConfiguration.integrations
            return integrationsMap.map { (sourceLakeName, destToIntegration )->
                val sourceLake = lakes.getValue(sourceLakeName)
                val value = Multimaps.asMap(destToIntegration).map { ( destinationName, integrations ) ->
                    val extIntegrations = integrations.filter { !it.gluttony } + integrations
                            .filter { it.gluttony }
                            .flatMap { integration ->
                                val destLake = lakes.getValue(destinationName)
                                BasePostgresIterable(
                                        StatementHolderSupplier(destLake.getHikariDatasource(), integration.source)
                                ) { rs ->
                                    //TODO: Add support for reading gluttony flag, master sql, upsert sql.
                                    Integration(
                                            rs.getString("description"),
                                            rs.getString("query"),
                                            rs.getString("destination")
                                    )
                                }
                            }

                    val paths = extIntegrations.map { integration ->
                        val destination = lakes.getValue(destinationName)
                        val start = OffsetDateTime.now()
                        launchLogger.logStarted(integrationConfiguration.name, destination, integration, start)
                        val ds = try {
                            logger.info("Transferring ${sourceLake.name} with query ${integration.source}")
                            getSourceDataset(sourceLake, integration, session)
                        } catch (ex: Exception) {
                            launchLogger.logFailed(sourceLakeName, destination, integration, start)
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

                        val sparkWriter = when (destination.dataFormat) {
                            CSV_FORMAT, LEGACY_CSV_FORMAT -> {
                                ds.write()
                                        .option("header", true)
                                        .format(CSV_FORMAT)
                            }
                            ORC_FORMAT -> {
                                ds.write().format(ORC_FORMAT)
                            }
                            else -> {
                                ds.write()
                                        .option("batchsize", destination.batchSize.toLong())
                                        .option("driver", destination.driver)
                                        .mode(destination.writeMode)
                                        .format("jdbc")
                            }
                        }
                        logger.info("Created spark writer for destination: {}", destination)
                        val ctxt = timer.time()
                        val destinationPath = when (destination.driver) {
                            FILESYSTEM_DRIVER, S3_DRIVER -> {
                                val fileName = "${integration.destination}-${OffsetDateTime.now(Clock.systemUTC())}"
                                sparkWriter.save("${destination.url}/$fileName")
                                fileName
                            }
                            else -> {
                                toDatabase(integrationConfiguration.name, sparkWriter, destination, integration, start)
                                integration.destination
                            }
                        }
                        val elapsedNs = ctxt.stop()
                        val secs = elapsedNs / 1_000_000_000.0
                        val mins = secs / 60.0
                        logger.info("Finished writing to name: {} in {} seconds ({} minutes)", destination, secs, mins)
                        destinationPath
                    }
                    destinationName to paths
                }.toMap()
                sourceLakeName to value
            }.toMap()
        }

        private fun mergeIntoMaster(
                destination: DataLake,
                integrationName: String,
                integration: Integration,
                start: OffsetDateTime
        ) {
            if (integration.masterTableSql.isBlank() || integration.mergeSql.isBlank()) return
            try {
                destination.getHikariDatasource().connection.use { conn ->
                    conn.createStatement().use { stmt ->
                        //Make sure master table exists and insert.
                        stmt.execute(integration.masterTableSql)
                        stmt.execute(integration.mergeSql)
                    }
                }
            } catch (ex: Exception) {
                launchLogger.logFailed(integrationName, destination, integration, start)
                logger.error(
                        "Integration {} failed going from {} to {} while merging to master. Exiting.",
                        integrationName,
                        integration.source,
                        integration.destination,
                        ex
                )
            }
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
            //Try and merge any data from a failed previous run. Merge after success
            try {
                mergeIntoMaster(destination, integrationName, integration, start)
                ds.jdbc(
                        destination.url,
                        integration.destination,
                        destination.properties
                )
                mergeIntoMaster(destination, integrationName, integration, start)
                launchLogger.logSuccessful(integrationName, destination, integration, start)
            } catch (ex: Exception) {
                launchLogger.logFailed(integrationName, destination, integration, start)
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

        @JvmStatic
        fun getSourceDataset(datasource: DataLake, integration: Integration, sparkSession: SparkSession): Dataset<Row> {
            return getDataset(datasource, integration.source, sparkSession)
        }

        @JvmStatic
        fun getDataset(
                lake: DataLake, fileOrTableName: String, sparkSession: SparkSession, knownHeader: Boolean = false
        ): Dataset<Row> {
            when (lake.dataFormat) {
                CSV_FORMAT, LEGACY_CSV_FORMAT -> return sparkSession
                        .read()
                        .option("header", if (knownHeader) true else lake.header)
                        .option("inferSchema", true)
                        .csv("${lake.url}/$fileOrTableName")
                ORC_FORMAT -> return sparkSession
                        .read()
                        .option("inferSchema", true)
                        .orc("${lake.url}/$fileOrTableName")
                else -> return sparkSession
                        .read()
                        .format("jdbc")
                        .option("url", lake.url)
                        .option("dbtable", fileOrTableName)
                        .option("user", lake.username)
                        .option("password", lake.password)
                        .option("driver", lake.driver)
                        .option("fetchSize", lake.fetchSize.toLong())
                        .load()
            }
        }
    }
}
