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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Preconditions
import com.google.common.collect.ListMultimap
import com.google.common.collect.Sets
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory
import java.util.*

private const val DATASOURCES       = "datasources"
private const val DESTINATIONS      = "destinations"
private const val INTEGRATIONS      = "integrations"
private const val AWS_CLIENT        = "awsClient"
private const val DATA_LAKES        = "datalakes"

const val URL            = "url"
const val NAME           = "name"
const val USER           = "user"
const val USERNAME       = "username"
const val DRIVER         = "driver"
const val PASSWORD       = "password"
const val PROPERTIES     = "properties"
const val WRITE_MODE     = "writeMode"
const val BATCH_SIZE     = "batchSize"
const val FETCH_SIZE     = "fetchSize"
const val DATA_FORMAT    = "dataFormat"
const val DESCRIPTION    = "description"
const val DESTINATION    = "destination"
const val SOURCE            = "source"

// ported from old classes
const val HEADER             = "header"
const val JDBC_URL           = "jdbcUrl"
const val MAXIMUM_POOL_SIZE  = "maximumPoolSize"
const val CONNECTION_TIMEOUT = "connectionTimeout"

const val LEGACY_CSV_FORMAT         = "com.openlattice.launchpad.csv"
const val CSV_FORMAT                = "csv"
const val ORC_FORMAT                = "orc"
const val FILESYSTEM_DRIVER         = "filesystem"
const val S3_DRIVER                 = "s3"
const val UNKNOWN                   = "unknown"


const val DEFAULT_DATA_CHUNK_SIZE   = 20_000

@JvmField
val DEFAULT_WRITE_MODE = SaveMode.Overwrite

@JvmField
val NON_JDBC_DRIVERS: Set<String>   = Sets.newHashSet(S3_DRIVER, FILESYSTEM_DRIVER)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class IntegrationConfiguration(
        @JsonProperty(NAME) val name: String,
        @JsonProperty(DESCRIPTION) val description: String,
        @JsonProperty(AWS_CLIENT) val awsConfig: Optional<AwsS3ClientConfiguration>,
        @JsonProperty(DATASOURCES) val datasources: Optional<List<LaunchpadDatasource>>,
        @JsonProperty(DESTINATIONS) val destinations: Optional<List<LaunchpadDestination>>,
        @JsonProperty(DATA_LAKES) val datalakes: Optional<List<DataLake>>,
        @JsonProperty(INTEGRATIONS) val integrations: Map<String, ListMultimap<String, Integration>>
) {
    init {
        Preconditions.checkState( ( datasources.isPresent && destinations.isPresent ) || datalakes.isPresent,
        "Must specify either a datasources and a destination or one or more data lakes")
    }
}

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class Integration(
        @JsonProperty(DESCRIPTION) val description : String = "",
        @JsonProperty(SOURCE) val source: String,
        @JsonProperty(DESTINATION) val destination: String,
        @JsonProperty("gluttony") val gluttony : Boolean = false
)

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
data class AwsS3ClientConfiguration(
        @JsonProperty("regionName") val regionName: String,
        @JsonProperty("accessKeyId") val accessKeyId: String,
        @JsonProperty("secretAccessKey") val secretAccessKey: String
)

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
data class DataLake(
        @JsonProperty(NAME) val name: String,
        @JsonProperty(URL) val url: String,
        @JsonProperty(DRIVER) val driver: String,
        @JsonProperty(DATA_FORMAT) val dataFormat: String = driver,
        @JsonProperty(USERNAME) val user: String = "",
        @JsonProperty(PASSWORD) val password: String = "",
        @JsonProperty(HEADER) val header: Boolean = false,
        @JsonProperty(FETCH_SIZE) val fetchSize: Int = DEFAULT_DATA_CHUNK_SIZE,
        @JsonProperty(BATCH_SIZE) val batchSize: Int = DEFAULT_DATA_CHUNK_SIZE,
        @JsonProperty(WRITE_MODE) val writeMode: SaveMode = DEFAULT_WRITE_MODE,
        @JsonProperty("remoteLogging") val remoteLogger: Boolean = false,
        @JsonProperty(PROPERTIES) val properties: Properties = Properties()
) {
    companion object  {
        private val logger = LoggerFactory.getLogger(DataLake::class.java)
    }

    init {
        Preconditions.checkState(StringUtils.isNotBlank(name), "Name must not be blank.")
        when( driver ) {
            FILESYSTEM_DRIVER -> {
                logger.info("fs")
            }
            S3_DRIVER -> {
                logger.info("S3")
            }
            else -> {
                logger.info("other: $driver")
                Preconditions.checkState(StringUtils.isNotBlank(user), "Username cannot be blank for database connections.")
                if ( password == "" ){
                    logger.warn("Connecting to $name with a blank password.")
                }
            }
        }
    }

    @JsonIgnore
    fun getHikariDatasource(): HikariDataSource {
        val pClone = properties.clone() as Properties
        pClone.setProperty(USERNAME, pClone.getProperty(USER))
        pClone.remove(USER)
        val hc = HikariConfig(pClone)
        logger.info("JDBC URL = {}", hc.jdbcUrl)
        return HikariDataSource(hc)
    }
}
