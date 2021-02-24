package com.openlattice.launchpad

import com.amazonaws.services.costandusagereport.model.AWSRegion
import com.google.common.base.Preconditions
import com.google.common.collect.ListMultimap
import com.google.common.collect.Lists
import com.openlattice.launchpad.configuration.Archive
import com.openlattice.launchpad.configuration.DataLake
import com.openlattice.launchpad.configuration.Integration
import com.openlattice.launchpad.configuration.IntegrationConfiguration
import com.openlattice.launchpad.configuration.configureOrGetSparkSession
import com.openlattice.launchpad.serialization.JacksonSerializationConfiguration
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.util.Optional
import kotlin.system.exitProcess

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class Launchpad {
    companion object {
        private val logger = LoggerFactory.getLogger(Launchpad::class.java)

        @Throws(IOException::class)
        @JvmStatic
        public fun main(args: Array<String>) {
            val cl = LaunchpadCli.parseCommandLine(args)

            if (cl.hasOption(LaunchpadCli.HELP)) {
                LaunchpadCli.printHelp()
                exitProcess(0)
            }

            Preconditions.checkArgument(cl.hasOption(LaunchpadCli.FILE), "Integration file must be specified!")

            val integrationFilePath = cl.getOptionValue(LaunchpadCli.FILE)
            Preconditions.checkState(StringUtils.isNotBlank(integrationFilePath))
            val integrationFile = File(integrationFilePath)

            val config: IntegrationConfiguration
            try {
                config = JacksonSerializationConfiguration.yamlMapper.readValue(
                        integrationFile,
                        IntegrationConfiguration::class.java)
            } catch (ex: Exception) {
                println("There was an error parsing your integration configuration file. " +
                        "Please check your configuration file formatting and run launchpad again")
                ex.printStackTrace()
                exitProcess(-1)
            }

            val currentLakes: Optional<List<DataLake>> = config.datalakes
            if (!currentLakes.isPresent || currentLakes.get().isEmpty()) {
                val newConfig = convertToDataLakes(config)
                val newJson = JacksonSerializationConfiguration.yamlMapper.writeValueAsString(newConfig)
                println("The configuration format has been updated. " +
                        "Please replace your integration configuration file with the below contents and run launchpad again")
                println(newJson)
                exitProcess(-1)
            }

            validateDataLakes(config)

            validateAwsConfig(config)

            val integrations: Map<String, ListMultimap<String, Integration>> = config.integrations
            val archives: Map<String, Map<String, List<Archive>>> = config.archives

            val runIntegrations = if (integrations.isEmpty()) {
                if (archives.isEmpty()) {
                    println("Either integrations or archives must be specified to run launchpad")
                    exitProcess(-1)
                }
                println("Setting up archive run with launchpad")
                false
            } else {
                if (archives.isNotEmpty()) {
                    println("Only one of [ integrations, archives ] can be specified to run launchpad")
                    exitProcess(-1)
                }
                println("Setting up integration run with launchpad")
                true
            }

            try {
                configureOrGetSparkSession(config).use { session ->
                    if (runIntegrations) {
                        IntegrationRunner.runIntegrations(config, session)
                    } else {
                        ArchiveRunner.runArchives(config, session)
                    }
                }
            } catch (ex: java.lang.Exception) {
                logger.error("Exception running launchpad integration", ex)
            }
        }

        private fun validateAwsConfig(config: IntegrationConfiguration): Boolean {
            val maybeAws = config.awsConfig
            if (maybeAws.isEmpty) {
                return true
            }
            val aws = maybeAws.get()
            val conditions = aws.accessKeyId.isNotEmpty() && aws.secretAccessKey.isNotEmpty() && aws.regionName.isNotEmpty()
            return conditions && try {
                AWSRegion.valueOf(aws.regionName)
                true
            } catch (ex: Exception) {
                logger.error("The AWS region specified in the integration config file appears to be invalid", ex)
                false
            }
        }

        private fun validateDataLakes(config: IntegrationConfiguration): Boolean {
            val maybeLakes = config.datalakes
            if (maybeLakes.isEmpty) {
                return true
            }

            return maybeLakes.get().all { it.isValid() }
        }

        private fun convertToDataLakes(config: IntegrationConfiguration): IntegrationConfiguration? {
            val currentLakes = config.datalakes
            if (!currentLakes.isPresent || currentLakes.get().isEmpty()) {
                val lakes: MutableList<DataLake> = Lists.newArrayList()
                for (source in config.datasources.get()) {
                    lakes.add(source.asDataLake())
                }
                for (dest in config.destinations.get()) {
                    lakes.add(dest.asDataLake())
                }
                return IntegrationConfiguration(
                        config.name,
                        config.description,
                        config.awsConfig,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(lakes),
                        config.integrations,
                        mapOf()
                )
            }
            return config
        }
    }
}