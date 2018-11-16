/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.launchpad.configuration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.module.afterburner.AfterburnerModule
import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.Multimaps
import com.openlattice.launchpad.LaunchPad.CSV_DRIVER
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.io.File

/**
 *
 */
@SuppressFBWarnings(value = ["BC"], justification = "No cast found")
class IntegrationRunner {
    companion object {
        private val logger = LoggerFactory.getLogger(IntegrationRunner::class.java)
        private val sparkSession = SparkSession.builder()
                .master("local[${Runtime.getRuntime().availableProcessors()}]")
                .appName("integration")
                .orCreate

        @VisibleForTesting
        @JvmStatic
        fun runIntegrations(integrationConfiguration: IntegrationConfiguration) {
            val integrations = integrationConfiguration.integrations
            val datasources = integrationConfiguration.datasources.map { it.name to it }.toMap()
            val destinations = integrationConfiguration.destinations.map { it.name to it }.toMap()


            integrations.forEach { datasourceName, destinationsForDatasource ->
                val datasource = datasources[datasourceName]!!

                Multimaps.asMap(destinationsForDatasource).forEach { destinationName, integrations ->
                    integrations.forEach { integration ->
                        val destination = destinations[destinationName]!!

                        logger.info("Running integration: {}", integration)

                        val ds = getSourceDataset(datasource, integration)
                        logger.info("Read from source: {}", datasource)
                        //Only CSV and JDBC are tested.
                        when (destination.writeDriver) {
                            CSV_DRIVER -> ds.write().option("header", true).csv(destination.writeUrl)
                            "parquet" -> ds.write().parquet(destination.writeUrl)
                            "orc" -> ds.write().orc(destination.writeUrl)
                            else -> ds.write()
                                    .option("batchsize", destination.batchSize.toLong())
                                    .option("driver", destination.writeDriver)
                                    .mode(SaveMode.Overwrite)
                                    .jdbc(
                                            destination.writeUrl,
                                            integration.destination,
                                            destination.properties
                                    )
                        }
                        logger.info("Wrote to name: {}", destination)
                    }
                }
            }
        }

        @JvmStatic
        private fun getSourceDataset(datasource: LaunchpadDatasource, integration: Integration): Dataset<Row> {
            when (datasource.driver) {
                CSV_DRIVER -> return sparkSession
                        .read()
                        .option("header", true)
                        .option("inferSchema", true)
                        .csv(datasource.url + integration.source)
                else -> return sparkSession
                        .read()
                        .format("jdbc")
                        .option("url", datasource.url)
                        .option("dbtable", integration.source)
                        .option("user", datasource.user)
                        .option("password", datasource.password)
                        .option("driver", datasource.driver)
                        .option("fetchSize", datasource.fetchSize.toLong())
                        .load()
            }
        }
    }
}
