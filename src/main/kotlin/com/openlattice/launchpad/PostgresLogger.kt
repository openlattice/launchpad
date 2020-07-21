package com.openlattice.launchpad

import com.openlattice.launchpad.configuration.DataLake
import com.openlattice.launchpad.configuration.Integration
import com.openlattice.launchpad.configuration.IntegrationTables
import com.zaxxer.hikari.HikariDataSource
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.time.OffsetDateTime
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class LaunchpadLogger(
        val hds: HikariDataSource,
        val hostName: String = getHostname()
) {

    companion object {
        private val logger = LoggerFactory.getLogger(LaunchpadLogger::class.java)

        @JvmStatic
        fun fromLake( lake: DataLake ): LaunchpadLogger {
            return LaunchpadLogger( lake.getHikariDatasource() )
        }

        @JvmStatic
        private final fun getHostname(): String {
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
    }

    fun createLoggingTable() {
        hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(IntegrationTables.CREATE_INTEGRATION_ACTIVITY_SQL)
            }
        }
    }

    fun logFailed(
            integrationName: String,
            destination: DataLake,
            integration: Integration,
            start: OffsetDateTime
    ) {
        logOrWarn(
                IntegrationTables.LOG_FAILED_INTEGRATION,
                "Integration failed",
                "Unable to log failure to database. Terminating",
                integrationName,
                destination,
                integration,
                start
        )
    }

    fun logStarted(
            integrationName: String,
            destination: DataLake,
            integration: Integration,
            start: OffsetDateTime
    ) {
        logOrWarn(
                IntegrationTables.LOG_INTEGRATION_STARTED,
                "Starting integration $integrationName to ${destination.name}",
                "Unable to create activity entry in the database. Continuing data transfer...",
                integrationName,
                destination,
                integration,
                start
        )
    }

    fun logSuccessful(
            integrationName: String,
            destination: DataLake,
            integration: Integration,
            start: OffsetDateTime
    ){
        logOrWarn(
                IntegrationTables.LOG_SUCCESSFUL_INTEGRATION,
                "Integration succeeded",
                "Unable to log success to database. Continuing data transfer...",
                integrationName,
                destination,
                integration,
                start
        )
    }

    private fun logOrWarn(
            logSQL: String,
            consoleLoggerString: String,
            failureText: String,
            integrationName: String,
            destination: DataLake,
            integration: Integration,
            start: OffsetDateTime
    ) {
        if (!destination.latticeLogger) {
            logger.info(consoleLoggerString)
            return
        }
        try {
            unsafeExecuteSql(
                    logSQL,
                    integrationName,
                    integration,
                    start
            )
        } catch (ex: Exception) {
            logger.warn(failureText, ex)
        }
    }

    @SuppressFBWarnings(value = ["OBL_UNSATISFIED_OBLIGATION"], justification = "Spotbugs doesn't like kotlin")
    private fun unsafeExecuteSql(
            sql: String,
            integrationName: String,
            integration: Integration,
            start: OffsetDateTime
    ) {
        hds.connection.use { connection ->
            connection.prepareStatement(sql).use { ps ->
                ps.setString(1, integrationName)
                ps.setString(2, hostName)
                ps.setString(3, integration.destination)
                ps.setObject(4, start)
                ps.executeUpdate()
            }
        }
    }
}