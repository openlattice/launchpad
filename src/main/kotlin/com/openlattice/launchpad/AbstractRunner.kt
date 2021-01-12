package com.openlattice.launchpad

import com.codahale.metrics.MetricRegistry
import com.openlattice.launchpad.configuration.Constants.CSV_FORMAT
import com.openlattice.launchpad.configuration.Constants.FILESYSTEM_DRIVER
import com.openlattice.launchpad.configuration.Constants.LEGACY_CSV_FORMAT
import com.openlattice.launchpad.configuration.Constants.ORC_FORMAT
import com.openlattice.launchpad.configuration.Constants.S3_DRIVER
import com.openlattice.launchpad.configuration.DataLake
import com.openlattice.launchpad.configuration.IntegrationConfiguration
import com.openlattice.launchpad.configuration.Transferable
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.net.SocketTimeoutException
import java.nio.file.Paths
import java.time.Clock
import java.time.OffsetDateTime

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class AbstractRunner {
    companion object {

        private val logger = LoggerFactory.getLogger(AbstractRunner::class.java)

        private val timer = MetricRegistry().timer("uploadTime")

        /**
         * Writes to spark [session] transferring data from [sourceLake] to [destination],
         * logging to a database using [launchLogger].
         * Uses information in [configuration] to launch [transferrables] from LaunchPad to LandingPad
         * Returns the final destination of the data after landing
         */
        @JvmStatic
        fun writeUsingSpark(
                configuration: IntegrationConfiguration,
                sourceLake: DataLake,
                destination: DataLake,
                transferables: List<Transferable>,
                session: SparkSession,
                launchLogger: LaunchpadLogger
        ): List<String> {
            return transferables.map { transferable ->
                val launchPadQuery = transferable.getLaunchPad()
                val landingPadDestination = transferable.getLandingPad()

                val start = OffsetDateTime.now()
                launchLogger.logStarted(configuration.name, landingPadDestination, start, configuration)
                val ds = try {
                    logger.info("Transferring ${sourceLake.name} with query $launchPadQuery")
                    getSourceDataset(sourceLake, transferable, session)
                } catch (ex: Exception) {
                    launchLogger.logFailed(sourceLake.name, landingPadDestination, start, ex)
                    logger.error(
                            "Integration {} failed going from {} to {}. Exiting.",
                            configuration.name,
                            transferable.getSourceName(),
                            landingPadDestination,
                            ex
                    )
                    kotlin.system.exitProcess(1)
                }
                logger.info("Read from source: {}", sourceLake.name)

                val sparkWriter = when (destination.dataFormat) {
                    CSV_FORMAT, LEGACY_CSV_FORMAT -> {
                        val writer = ds.repartition(1)
                                .write()
                                .option("header", true)
                                .format(CSV_FORMAT)
                        if ( transferable.getBucketColumn() != null ){
                            writer.partitionBy("bucket_val")
                        } else {
                            writer
                        }
                    }
                    ORC_FORMAT -> {
                        val writer = ds.repartition(1)
                                .write()
                                .format(ORC_FORMAT)
                        if ( transferable.getBucketColumn() != null ){
                            writer.partitionBy("bucket_val")
                        } else {
                            writer
                        }
                    }
                    else -> {
                        ds.write()
                                .option("batchsize", destination.batchSize.toLong())
                                .option("driver", destination.driver)
                                .mode(destination.writeMode)
                                .format("jdbc")
                    }
                }
                logger.info("Created spark writer for destination: {}", destination.name)
                val ctxt = timer.time()
                val destinationPath = when (destination.driver) {
                    FILESYSTEM_DRIVER -> {
                        val fileName = "$landingPadDestination-${OffsetDateTime.now(Clock.systemUTC())}"
                        sparkWriter.save( Paths.get( destination.url, fileName ).toString())
                        fileName
                    }
                    S3_DRIVER -> {
                        sparkWriter.save( "${destination.url}/$landingPadDestination" )
                        landingPadDestination
                    }
                    else -> {
                        toDatabase(configuration.name, sparkWriter, destination, transferable, start, launchLogger)
                        transferable.getLandingPad()
                    }
                }
                val elapsedNs = ctxt.stop()
                val secs = elapsedNs / 1_000_000_000.0
                val mins = secs / 60.0
                logger.info("Finished writing to: {} in {} seconds ({} minutes)", destination.name, secs, mins)
                destinationPath
            }
        }

        private fun mergeIntoMaster(
                destination: DataLake,
                integrationName: String,
                integration: Transferable,
                start: OffsetDateTime,
                launchLogger: LaunchpadLogger
        ) {
            if (integration.getMasterTableSql().isBlank() || integration.getMergeTableSql().isBlank()) return
            try {
                destination.getHikariDatasource().connection.use { conn ->
                    conn.createStatement().use { stmt ->
                        //Make sure master table exists and insert.
                        stmt.execute(integration.getMasterTableSql())
                        stmt.execute(integration.getMergeTableSql())
                    }
                }
            } catch (ex: Exception) {
                launchLogger.logFailed(integrationName, integration.getLandingPad(), start, ex)
                logger.error(
                        "Integration {} failed going from {} to {} while merging to master. Exiting.",
                        integrationName,
                        integration.getLaunchPad(),
                        integration.getLandingPad(),
                        ex
                )
            }
        }

        @JvmStatic
        fun getSourceDataset(datasource: DataLake, transferable: Transferable, sparkSession: SparkSession): Dataset<Row> {
            return getDataset(datasource, transferable.getLaunchPad(), sparkSession)
        }

        @SuppressFBWarnings(
                value = ["DM_EXIT"],
                justification = "Intentionally shutting down JVM on terminal error"
        )
        @JvmStatic
        fun toDatabase(
                integrationName: String,
                ds: DataFrameWriter<Row>,
                destination: DataLake,
                transferable: Transferable,
                start: OffsetDateTime,
                launchLogger: LaunchpadLogger
        ) {
            //Try and merge any data from a failed previous run. Merge after success
            try {
                mergeIntoMaster(destination, integrationName, transferable, start, launchLogger)
                ds.jdbc(
                        destination.url,
                        transferable.getLandingPad(),
                        destination.properties
                )
                mergeIntoMaster(destination, integrationName, transferable, start, launchLogger)
                launchLogger.logSuccessful(integrationName, transferable.getLandingPad(), start)
            } catch (ex: Exception) {
                launchLogger.logFailed(integrationName, transferable.getLandingPad(), start, ex)
                logger.error(
                        "Integration {} failed going from {} to {}. Exiting.",
                        integrationName,
                        transferable.getLaunchPad(),
                        transferable.getLandingPad(),
                        ex
                )

                kotlin.system.exitProcess(1)
            }
        }

        @JvmStatic
        internal fun getDataset(
                lake: DataLake, fileOrTableName: String, sparkSession: SparkSession
        ): Dataset<Row> {
            logger.info("reading from $fileOrTableName")
            when (lake.dataFormat) {
                CSV_FORMAT, LEGACY_CSV_FORMAT -> return sparkSession
                        .read()
                        .option("header", lake.header)
                        .option("inferSchema", !lake.header )
                        .csv(Paths.get(lake.url, fileOrTableName).toString())
                ORC_FORMAT -> return sparkSession
                        .read()
                        .option("inferSchema", true)
                        .orc("${lake.url}/$fileOrTableName")
                else -> {
                    val session =  sparkSession
                            .read()
                            .format("jdbc")
                            .option("url", lake.url)
                            .option("dbtable", fileOrTableName)
                            .option("user", lake.username)
                            .option("password", lake.password)
                            .option("driver", lake.driver)
                            .option("fetchSize", lake.fetchSize.toLong())
                    try {
                        return session.load()
                    } catch ( ex: Exception ) {
                        logger.error("Connection to ${lake.url} not available, failing")
                        throw ex
                    }
                }
            }
        }
    }
}