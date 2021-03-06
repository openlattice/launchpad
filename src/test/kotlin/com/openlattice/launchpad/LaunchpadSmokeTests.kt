package com.openlattice.launchpad

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.openlattice.launchpad.configuration.Constants
import com.openlattice.launchpad.configuration.IntegrationConfiguration
import com.openlattice.launchpad.configuration.configureOrGetSparkSession
import com.openlattice.launchpad.serialization.JacksonSerializationConfiguration
import org.junit.Assert
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
        fun runArchiveValidateAndCleanup(config: IntegrationConfiguration, vararg sortColumn: String) {
            configureOrGetSparkSession(config).use { session ->
                val archivePaths = ArchiveRunner.runArchives(config, session)
//                IntegrationValidator.validateIntegration( config, integrationPaths, *sortColumn )
                cleanupAfterTest(config, archivePaths)
            }
        }

        @JvmStatic
        fun runIntegrationValidateAndCleanup(config: IntegrationConfiguration, vararg sortColumn: String) {
            configureOrGetSparkSession(config).use { session ->
                val integrationPaths = IntegrationRunner.runIntegrations(config, session)
                IntegrationValidator.validateIntegration(config, integrationPaths, *sortColumn)
                cleanupAfterTest(config, integrationPaths)
            }
        }

        @JvmStatic
        fun cleanupAfterTest(
                config: IntegrationConfiguration,
                integrationPaths: Map<String, Map<String, List<String>>>
        ) {
            val lakes = config.convertToDataLakesIfPresent()
            integrationPaths.forEach { (_, destToPaths) ->
                destToPaths.forEach { (dest, paths) ->
                    paths.forEach { path ->
                        val destination = lakes.getValue(dest)
                        when (destination.driver) {
                            Constants.FILESYSTEM_DRIVER -> {
                                // fs => delete dest file/folder
                                println("deleting file/folder from fs at ${destination.url}/$path")
                                if (!Paths.get("${destination.url}/$path").toFile().deleteRecursively()) {
                                    println("failed to delete file/folder from fs at ${destination.url}/$path")
                                }
                            }
                            Constants.S3_DRIVER -> {
                                // s3 => delete dest file/folder
                                val s3Client = AmazonS3ClientBuilder.standard()
                                        .withRegion(config.awsConfig.get().regionName)
                                        .withCredentials(DefaultAWSCredentialsProviderChain())
                                        .build()

                                val parts = URI(destination.url).schemeSpecificPart.split('/').iterator()
                                var partsNxt = parts.next()
                                while (partsNxt.isBlank()) {
                                    partsNxt = parts.next()
                                }
                                val bucket = partsNxt
                                val rest = StringBuilder()
                                while (parts.hasNext()) {
                                    rest.append(parts.next())
                                    rest.append('/')
                                }
                                rest.append(path)
                                println("deleting from bucket: $bucket key: $rest")

                                val listResults = s3Client.listObjectsV2(bucket, rest.toString())
                                listResults.objectSummaries.forEach { summary ->
                                    try {
                                        s3Client.deleteObject(summary.bucketName, summary.key)
                                    } catch (ex: Exception) {
                                        ex.printStackTrace()
                                    }
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
    fun testJacksonFilterSerialziation() {
        val dataLakeConfig = IntegrationConfigLoader.fromJdbc.toS3.orcFormat()
        var asString = JacksonSerializationConfiguration.credentialFilteredJsonMapper.writeValueAsString(dataLakeConfig)
        println(asString)
        Assert.assertTrue(!asString.contains("testSecretAC"))
        Assert.assertTrue(!asString.contains("testACID"))
        Assert.assertTrue(!asString.contains("example_user"))
        Assert.assertTrue(!asString.contains("examplepassword"))

        val legacyConfig = IntegrationConfigLoader.fromJdbc.toJdbc.implicitFormat()
        asString = JacksonSerializationConfiguration.credentialFilteredJsonMapper.writeValueAsString(legacyConfig)
        println(asString)
        Assert.assertTrue(!asString.contains("example_user"))
        Assert.assertTrue(!asString.contains("examplepassword"))
        Assert.assertTrue(!asString.contains("oltest"))
        Assert.assertTrue(!asString.contains("test"))
    }

    @Test
    @Throws(IOException::class)
    fun runJdbcJdbcIntegration() {
        val config = IntegrationConfigLoader.fromJdbc.toJdbc.implicitFormat()
        runIntegrationValidateAndCleanup(config, "SubjectIdentification", "IncidentID")
    }

    @Test
    @Throws(IOException::class)
    fun runJdbcFsOrcIntegration() {
        val config = IntegrationConfigLoader.fromJdbc.toFs.orcFormat()
        runIntegrationValidateAndCleanup(config, "SubjectIdentification", "IncidentID")
    }

    @Test
    @Throws(IOException::class)
    fun runJdbcS3OrcIntegration() {
        val config = IntegrationConfigLoader.fromJdbc.toS3.orcFormat()
        runIntegrationValidateAndCleanup(config, "SubjectIdentification", "IncidentID")
    }

    @Ignore
    @Throws(IOException::class)
    fun runJdbcFsOrcArchive() {
        val config = IntegrationConfigLoader.fromJdbc.toFs.archiveOrcFormat()
        runArchiveValidateAndCleanup(config, "SubjectIdentification", "IncidentID")
    }

    @Ignore
    @Throws(IOException::class)
    fun runJdbcS3OrcArchive() {
        val config = IntegrationConfigLoader.fromJdbc.toS3.archiveOrcFormat()
        runArchiveValidateAndCleanup(config, "SubjectIdentification", "IncidentID")
    }

    @Ignore
    @Throws(IOException::class)
    fun runJdbcFsCsvIntegration() {
        val config = IntegrationConfigLoader.fromJdbc.toFs.csvFormat()
        runIntegrationValidateAndCleanup(config, "SubjectIdentification", "IncidentID")
    }

    @Ignore
    @Throws(IOException::class)
    fun runJdbcOracleIntegration() {
        val config = IntegrationConfigLoader.fromJdbc.toOracle.implicitFormat()
        runIntegrationValidateAndCleanup(config, "SubjectIdentification", "IncidentID")
    }

    @Ignore
    @Throws(IOException::class)
    fun runJdbcJdbcAppendOnlyIntegration() {
        val config = IntegrationConfigLoader.fromJdbc.toJdbc.appendOnlyConfiguration()
        runIntegrationValidateAndCleanup(config, "SubjectIdentification", "IncidentID")
    }

    @Ignore
    @Throws(IOException::class)
    fun runJdbcS3CsvIntegration() {
        val config = IntegrationConfigLoader.fromJdbc.toS3.csvFormat()
        runIntegrationValidateAndCleanup(config, "SubjectIdentification", "IncidentID")
    }

    @Ignore
    @Throws(IOException::class)
    fun runFsCsvJdbcIntegration() {
        val config = IntegrationConfigLoader.fromCsv.toJdbc.implicitFormat()
        runIntegrationValidateAndCleanup(config, "SubjectIdentification", "IncidentID")
    }
}
