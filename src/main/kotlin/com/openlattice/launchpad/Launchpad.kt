package com.openlattice.launchpad

import com.google.common.base.Preconditions
import com.google.common.collect.ListMultimap
import com.google.common.collect.Lists
import com.openlattice.launchpad.configuration.*
import com.openlattice.launchpad.serialization.JacksonSerializationConfiguration
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.util.*
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
                println(
                        "There was an error parsing your integration configuration file. Please check your file and run launchpad again")
                ex.printStackTrace()
                exitProcess(-1)
            }

            val currentLakes: Optional<List<DataLake>> = config.datalakes
            if (!currentLakes.isPresent || currentLakes.get().isEmpty()) {
                val newConfig = convertToDataLakes(config)
                val newJson = JacksonSerializationConfiguration.yamlMapper.writeValueAsString(newConfig)
                println("Please replace your current yaml configuration file with the below yaml:")
                println(newJson)
                exitProcess(-1)
            }

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