package com.openlattice.launchpad

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.openlattice.launchpad.configuration.Constants
import com.openlattice.launchpad.configuration.IntegrationConfiguration
import com.openlattice.launchpad.configuration.IntegrationRunner
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import java.io.IOException
import java.net.URI
import java.nio.file.Paths

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
                                if ( !Paths.get("${destination.url}/$path").toFile().deleteRecursively() ) {
                                    println("failed to delete file/folder from fs at ${destination.url}/$path")
                                }
                            }
                            Constants.S3_DRIVER -> {
                                // s3 => delete dest file/folder
                                val awsS3Config = config.awsConfig.get()
                                val credsProvider = AWSStaticCredentialsProvider(
                                        BasicAWSCredentials(awsS3Config.accessKeyId, awsS3Config.secretAccessKey))
                               //InstanceProfileCredentialsProvider.createAsyncRefreshingProvider(true)
                                val s3Client = AmazonS3ClientBuilder.standard()
                                        .withRegion(awsS3Config.regionName)
                                        .withCredentials(credsProvider)
                                        .build()

                                val parts = URI(destination.url).schemeSpecificPart.split('/').iterator()
                                var partsNxt = parts.next()
                                while ( partsNxt.isBlank() ){
                                    partsNxt = parts.next()
                                }
                                val bucket = partsNxt
                                val rest = StringBuilder()
                                while (parts.hasNext()) {
                                    rest.append(parts.next())
                                    rest.append('/')
                                }
                                rest.append(path)
                                println("deleting from bucket: $bucket key: ${rest.toString()}")
                                try {
                                    s3Client.deleteObject(bucket, rest.toString())
                                } catch ( ex: Exception ) {
                                    ex.printStackTrace()
                                }
                            }
                            else -> {
                                // jdbc => drop dest table
                                val hds = destination.getHikariDatasource()
                                println("dropping table from ${destination.url}/$path")
                                hds.connection.use { conn ->
                                    conn.createStatement().execute("DROP TABLE $path;")
                                }
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
