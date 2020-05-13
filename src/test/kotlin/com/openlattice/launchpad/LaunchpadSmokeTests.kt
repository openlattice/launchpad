package com.openlattice.launchpad

import com.openlattice.launchpad.configuration.Constants
import com.openlattice.launchpad.configuration.IntegrationConfiguration
import com.openlattice.launchpad.configuration.IntegrationRunner
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import java.io.IOException

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class LaunchpadSmokeTests {
    @Before
    fun setup() {
        //connect to db and create
    }

    companion object {
        @JvmStatic
        fun runTestValidateAndCleanup(config: IntegrationConfiguration, vararg sortColumn: String ) {
            val integrationPaths = IntegrationRunner.runIntegrations(config)
            IntegrationValidator.validateIntegration( config, integrationPaths, *sortColumn )
            cleanupAfterTest(config, integrationPaths)
        }

        @JvmStatic
        fun cleanupAfterTest(config: IntegrationConfiguration, integrationPaths: Map<String, Map<String, List<String>>>) {
            val lakes = IntegrationRunner.convertToDataLakesIfPresent(config)
            integrationPaths.forEach { source, destToPaths ->
                destToPaths.forEach { dest, paths ->
                    paths.forEach { path ->
                        val destination = lakes.getValue( dest )
                        when ( destination.driver ){
                            Constants.FILESYSTEM_DRIVER -> {
                                // fs => delete dest file/folder
                                println("deleting file/folder from fs at ${destination.url}/$path")
                            }
                            Constants.S3_DRIVER -> {
                                // s3 => delete dest file/folder
                                println("deleting file/folder from s3 at ${destination.url}/$path")
                            }
                            else -> {
                                // jdbc => drop dest table
                                println("dropping table from ${destination.url}/$path")
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    @Throws(IOException::class)
    fun runJdbcJdbcIntegration() {
        val config = IntegrationConfigLoader.fromJdbc.toJdbc.implicitFormat()
        runTestValidateAndCleanup( config, "SubjectIdentification", "IncidentID")
    }

    @Test
    @Throws(IOException::class)
    fun runJdbcFsOrcIntegration() {
        val config = IntegrationConfigLoader.fromJdbc.toFs.orcFormat()
        runTestValidateAndCleanup( config, "SubjectIdentification", "IncidentID")
    }

    @Test
    @Throws(IOException::class)
    fun runJdbcS3OrcIntegration() {
        val config = IntegrationConfigLoader.fromJdbc.toS3.orcFormat()
        runTestValidateAndCleanup( config, "SubjectIdentification", "IncidentID")
    }

    @Ignore
    @Throws(IOException::class)
    fun runJdbcFsCsvIntegration() {
        val config = IntegrationConfigLoader.fromJdbc.toFs.csvFormat()
        runTestValidateAndCleanup( config, "SubjectIdentification", "IncidentID")
    }

    @Ignore
    @Throws(IOException::class)
    fun runJdbcOracleIntegration(){
        val config = IntegrationConfigLoader.fromJdbc.toOracle.implicitFormat()
        runTestValidateAndCleanup( config, "SubjectIdentification", "IncidentID")
    }

    @Ignore
    @Throws(IOException::class)
    fun runJdbcJdbcAppendOnlyIntegration() {
        val config = IntegrationConfigLoader.fromJdbc.toJdbc.appendOnlyConfiguration()
        runTestValidateAndCleanup( config, "SubjectIdentification", "IncidentID")
    }

    @Ignore
    @Throws(IOException::class)
    fun runJdbcS3CsvIntegration() {
        val config = IntegrationConfigLoader.fromJdbc.toS3.csvFormat()
        runTestValidateAndCleanup( config, "SubjectIdentification", "IncidentID")
    }
}
