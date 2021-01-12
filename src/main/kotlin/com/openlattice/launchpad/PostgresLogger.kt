package com.openlattice.launchpad

import com.openlattice.launchpad.configuration.DataLake
import com.openlattice.launchpad.configuration.IntegrationConfiguration
import com.openlattice.launchpad.configuration.IntegrationTables
import com.openlattice.launchpad.serialization.JacksonSerializationConfiguration
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.time.OffsetDateTime
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class LaunchpadLogger private constructor(
        private val maybeHds: HikariDataSource?,
        private val hostname: String = getHost()
) {

    private constructor(): this(null)

    init {
        if ( maybeHds != null) {
            createOrUpgradeLoggingTable( maybeHds )
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(LaunchpadLogger::class.java)

        @JvmStatic
        private fun getHost(): String {
            return try {
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
        }

        @JvmStatic
        fun createLogger( lakes: Map<String, DataLake>  ): LaunchpadLogger {
            val maybeLoggerSet = lakes.values.firstOrNull { it.latticeLogger } ?: return LaunchpadLogger()
            return LaunchpadLogger( maybeLoggerSet.getHikariDatasource() )
        }
    }

    fun createOrUpgradeLoggingTable( hds: HikariDataSource ) {
        hds.connection.use { connection ->
            connection.createStatement().use { stmt ->
                stmt.execute(IntegrationTables.CREATE_INTEGRATION_ACTIVITY_SQL)
            }
            try {
                logger.info("Upgrading integration status table")
                connection.autoCommit = false
                for ( upgrade in IntegrationTables.upgrades ) {
                    logger.info("Applying status table upgrade {}", upgrade)
                    connection.createStatement().use { stmt ->
                        stmt.execute( upgrade )
                    }
                }
                connection.commit()
            } catch ( ex: Exception ) {
                logger.error("Exception occurred while upgrading logging table, rolling back", ex)
                connection.rollback()
            } finally {
                connection.autoCommit = true
            }
        }
    }

    fun logStarted(
            integrationName: String,
            destinationTableName: String,
            start: OffsetDateTime,
            configuration: IntegrationConfiguration
    ) {
        logOrWarn(
                "Starting integration $integrationName data lake $destinationTableName",
                "Unable to create activity entry in the database. Continuing data transfer..."
        ) { hds, hostname ->
            val strippedConfigAsJson = JacksonSerializationConfiguration.credentialFilteredJsonMapper.writeValueAsString(configuration)
            hds.connection.use { connection ->
                connection.prepareStatement(IntegrationTables.LOG_INTEGRATION_STARTED).use { ps ->
                    ps.setString(1, integrationName)
                    ps.setString(2, hostname)
                    ps.setString(3, destinationTableName)
                    ps.setObject(4, start)
                    ps.setString(5, strippedConfigAsJson)
                    ps.executeUpdate()
                }
            }
        }
    }

    fun logSuccessful(
            integrationName: String,
            destinationTableName: String,
            start: OffsetDateTime
    ){
        logOrWarn(
                "Integration succeeded",
                "Unable to log success to database. Continuing data transfer..."
        ) { hds, hostname ->
            hds.connection.use { connection ->
                connection.prepareStatement(IntegrationTables.LOG_SUCCESSFUL_INTEGRATION).use { ps ->
                    ps.setString(1, integrationName)
                    ps.setString(2, hostname)
                    ps.setString(3, destinationTableName)
                    ps.setObject(4, start)
                    ps.executeUpdate()
                }
            }
        }
    }

    fun logFailed(
            integrationName: String,
            destinationTableName: String,
            start: OffsetDateTime,
            exception: Exception
    ) {
        logOrWarn(
                "Integration failed",
                "Unable to log failure to database. Terminating"
        ) { hds, hostname ->
            hds.connection.use { connection ->
                connection.prepareStatement(IntegrationTables.LOG_FAILED_INTEGRATION).use { ps ->
                    ps.setString(1, exception.toString())
                    ps.setString(2, integrationName)
                    ps.setString(3, hostname)
                    ps.setString(4, destinationTableName)
                    ps.setObject(5, start)
                    ps.executeUpdate()
                }
            }
        }
    }

    private fun logOrWarn(
            consoleLoggerString: String,
            failureText: String,
            block: (hds: HikariDataSource, hostname: String) -> Unit
    ) {
        logger.info(consoleLoggerString)
        if ( maybeHds == null ) {
            return
        }
        try {
            block(maybeHds, hostname)
        } catch (ex: Exception) {
            logger.error(failureText, ex)
        }
    }
}