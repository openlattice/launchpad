package com.openlattice.launchpad

import com.codahale.metrics.MetricRegistry
import com.google.common.collect.Multimaps
import com.openlattice.launchpad.configuration.IntegrationConfiguration
import com.openlattice.launchpad.configuration.configureOrGetSparkSession
import org.junit.Assert
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class IntegrationValidator {
    companion object {
        private val logger = LoggerFactory.getLogger(IntegrationRunner::class.java)

        private val timer = MetricRegistry().timer("validateTimer")

        private lateinit var launchLogger: LaunchpadLogger

        fun validateIntegration(
                integrationConfiguration: IntegrationConfiguration,
                integrationPaths: Map<String, Map<String, List<String>>>,
                vararg sortColumns: String
        ) {
            val integrationsMap = integrationConfiguration.integrations

            // map to lakes if needed. This should be removed once launchpads are upgraded
            val lakes = integrationConfiguration.convertToDataLakesIfPresent()

            launchLogger = LaunchpadLogger.createLogger( lakes )

            val session = configureOrGetSparkSession(integrationConfiguration)

            integrationsMap.forEach { (sourceLakeName, destToIntegration) ->
                val sourceLake = lakes.getValue( sourceLakeName )
                Multimaps.asMap(destToIntegration).forEach { (destinationLakeName, integrations) ->

                    val paths = integrationPaths.get(sourceLakeName)!!.get(destinationLakeName)!!.iterator()
                    integrations.filter {
                        !it.gluttony
                    }.forEach { integration ->
                        val destination = lakes.getValue(destinationLakeName)

                        logger.info("Validating integration: {}", integration)
                        val start = OffsetDateTime.now()
                        launchLogger.logStarted(integrationConfiguration.name, integration.destination, start, integrationConfiguration)
                        val sourceData = try {
                            logger.info("Reading ${sourceLake.name} with source query ${integration.source}")
                            AbstractRunner.getSourceDataset(sourceLake, integration, session)
                        } catch (ex: Exception) {
                            launchLogger.logFailed(sourceLakeName, integration.destination, start, ex)
                            logger.error(
                                    "Integration {} failed reading source {}. Exiting.",
                                    integrationConfiguration.name,
                                    integration.source,
                                    ex
                            )
                            kotlin.system.exitProcess(1)
                        }
                        logger.info("Read from source: {}", sourceLake)

                        val destinationData = try {
                            logger.info("Reading ${destination.name} with destination query ${integration.destination}")
                            AbstractRunner.getDataset(destination, paths.next(), session)
                        } catch (ex: Exception) {
                            launchLogger.logFailed(sourceLakeName, integration.destination, start, ex)
                            logger.error(
                                    "Integration {} failed reading destination {}. Exiting.",
                                    integrationConfiguration.name,
                                    integration.destination,
                                    ex
                            )

                            kotlin.system.exitProcess(1)
                        }
                        logger.info("Read from dest: {}", destination)

                        val ctxt = timer.time()
                        val destRows = arrayListOf<ArrayList<String>>()
                        var vals = arrayListOf<String>()
                        var j = 0
                        val first = sortColumns[0]
                        val rest = sortColumns.sliceArray(1..sortColumns.lastIndex)
                        destinationData.sort(first, *rest).takeAsList(12_000).forEach {destRow ->
                            for ( i in 0 until destRow.length()){
                                val destCol = destRow.get(i)
                                vals.add(i, "$destCol")
                            }
                            destRows.add(j, vals)
                            vals = arrayListOf()
                            j++
                        }

                        j = 0
                        sourceData.sort(first, *rest).takeAsList(12_000).forEach { sourceRow ->
                            val destRow = destRows.get(j)
                            for ( i in 0 until sourceRow.length()){
                                val destCol = destRow.get(i)
                                val srcCol = sourceRow.get(i)
                                Assert.assertEquals("comparing row $j, column $i", "$srcCol", "$destCol")
                            }
                            j++
                        }
                        val elapsedNs = ctxt.stop()
                        val secs = elapsedNs/1_000_000_000.0
                        val mins = secs/60.0
                        logger.info("Finished validating {} to {} in {} seconds ({} minutes)", sourceLake.url, destination.url, secs, mins)
                    }
                }
            }
        }
    }
}