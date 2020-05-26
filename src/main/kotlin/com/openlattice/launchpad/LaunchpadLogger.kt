package com.openlattice.launchpad

import com.openlattice.launchpad.configuration.CompletionState
import com.openlattice.launchpad.configuration.DataLake
import com.openlattice.launchpad.configuration.Integration
import com.openlattice.launchpad.configuration.IntegrationTables
import com.zaxxer.hikari.HikariDataSource
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.time.OffsetDateTime
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class LaunchpadLogger(val logger: Logger,
                      val integrationName: String,
                      val loggingLake: DataLake,
                      val hikariDataSource: HikariDataSource = loggingLake.getHikariDatasource()
) {

    companion object {
        private val lpLogger = LoggerFactory.getLogger(LaunchpadLogger::class.java)

        private val hostName = try {
            val localhost = InetAddress.getLocalHost()
            if (localhost.hostName.isBlank()) {
                localhost.hostAddress
            } else {
                localhost.hostName
            }
        } catch (ex: Exception) {
            val id = UUID.randomUUID()
            lpLogger.warn("Unable to get host for this machine. Using to random id: $id")
            id.toString()
        }
    }

    init {
        hikariDataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(IntegrationTables.CREATE_INTEGRATION_ACTIVITY_SQL)
            }
        }
    }

    fun logStarted(
            integration: Integration,
            start: OffsetDateTime
    ) {
        logWithOptions(
                IntegrationTables.LOG_INTEGRATION_STARTED,
                "Unable to create activity entry in the database. Continuing data transfer...",
                "Started integration {} going from {} to {}.",
                integration,
                start
        )
    }

    fun logSuccessful(
            integration: Integration,
            start: OffsetDateTime
    ) {
        logWithOptions(
                IntegrationTables.LOG_SUCCESSFUL_INTEGRATION,
                "Unable to log success to database. Continuing data transfer...",
                "Integration {} succeeded going from {} to {}.",
                integration,
                start
        )
    }

    fun logFailed(
            integration: Integration,
            start: OffsetDateTime,
            ex: Exception
    ) {
        try {
            unsafeExecuteSql(
                    IntegrationTables.LOG_FAILED_INTEGRATION,
                    integration,
                    start
            )
        } catch (ex: Exception) {
            logger.warn("Unable to log failure to database. Terminating", ex)
        }

        logger.error(
                "Integration {} failed going from {} to {}. Exiting.",
                integrationName,
                integration.source,
                integration.destination,
                ex
        )
    }

    private fun logWithOptions(
            loggingSql: String,
            exceptionMessage: String,
            logFileMessage: String,
            integration: Integration,
            start: OffsetDateTime
    ) {
        try {
            unsafeExecuteSql(
                    loggingSql,
                    integration,
                    start
            )
        } catch (ex: Exception) {
            logger.warn(exceptionMessage, ex)
        }

        logger.info(
                logFileMessage,
                integrationName,
                integration.source,
                integration.destination
        )
    }

    @SuppressFBWarnings(value = ["OBL_UNSATISFIED_OBLIGATION"], justification = "Spotbugs doesn't like kotlin")
    private fun unsafeExecuteSql(
            sql: String,
            integration: Integration,
            start: OffsetDateTime
    ) {
        hikariDataSource.connection.use { connection ->
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

class LaunchpadShutdownHook(
        val launchpadLogger: LaunchpadLogger,
        var currentState: CompletionState,
        val hikariDataSource: HikariDataSource = launchpadLogger.hikariDataSource
): Thread() {
    override fun run() {
        hikariDataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
//                stmt.execute()
            }
        }
    }
}