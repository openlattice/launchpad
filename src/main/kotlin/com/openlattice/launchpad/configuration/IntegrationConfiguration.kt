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
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Preconditions
import com.google.common.collect.ListMultimap
import com.openlattice.launchpad.configuration.Constants.BATCH_SIZE
import com.openlattice.launchpad.configuration.Constants.DATA_FORMAT
import com.openlattice.launchpad.configuration.Constants.DEFAULT_DATA_CHUNK_SIZE
import com.openlattice.launchpad.configuration.Constants.DEFAULT_WRITE_MODE
import com.openlattice.launchpad.configuration.Constants.DESCRIPTION
import com.openlattice.launchpad.configuration.Constants.DESTINATION
import com.openlattice.launchpad.configuration.Constants.DRIVER
import com.openlattice.launchpad.configuration.Constants.FETCH_SIZE
import com.openlattice.launchpad.configuration.Constants.FILESYSTEM_DRIVER
import com.openlattice.launchpad.configuration.Constants.HEADER
import com.openlattice.launchpad.configuration.Constants.LATTICE_LOGGER
import com.openlattice.launchpad.configuration.Constants.NAME
import com.openlattice.launchpad.configuration.Constants.PASSWORD
import com.openlattice.launchpad.configuration.Constants.PROPERTIES
import com.openlattice.launchpad.configuration.Constants.S3_DRIVER
import com.openlattice.launchpad.configuration.Constants.SOURCE
import com.openlattice.launchpad.configuration.Constants.URL
import com.openlattice.launchpad.configuration.Constants.USER
import com.openlattice.launchpad.configuration.Constants.USERNAME
import com.openlattice.launchpad.configuration.Constants.WRITE_MODE
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory
import java.util.*

private const val DATASOURCES       = "datasources"
private const val DESTINATIONS      = "destinations"
private const val INTEGRATIONS      = "integrations"
private const val AWS_CONFIG        = "awsConfig"
private const val DATA_LAKES        = "datalakes"
private const val GLUTTONY          = "gluttony"

private const val REGION_NAME       = "regionName"
private const val ACCESS_KEY_ID     = "accessKeyId"
private const val SECRET_ACCESS_KEY = "secretAccessKey"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
data class IntegrationConfiguration(
        @JsonProperty(NAME) val name: String,
        @JsonProperty(DESCRIPTION) val description: String,
        @JsonProperty(AWS_CONFIG) val awsConfig: Optional<AwsS3ClientConfiguration>,
        @JsonProperty(DATASOURCES) val datasources: Optional<List<LaunchpadDatasource>>,
        @JsonProperty(DESTINATIONS) val destinations: Optional<List<LaunchpadDestination>>,
        @JsonProperty(DATA_LAKES) val datalakes: Optional<List<DataLake>>,
        @JsonProperty(INTEGRATIONS) val integrations: Map<String, ListMultimap<String, Integration>>
) {
    init {
        Preconditions.checkState( ( datasources.isPresent && destinations.isPresent ) || datalakes.isPresent,
        "Must specify either one or more datasources and destinations or one or more data lakes")
    }
}

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
data class Integration(
        @JsonProperty(DESCRIPTION) val description : String = "",
        @JsonProperty(SOURCE) val source: String,
        @JsonProperty(DESTINATION) val destination: String,
        @JsonProperty(GLUTTONY) val gluttony : Boolean = false
)

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
data class AwsS3ClientConfiguration(
        @JsonProperty(REGION_NAME) val regionName: String,
        @JsonProperty(ACCESS_KEY_ID) val accessKeyId: String,
        @JsonProperty(SECRET_ACCESS_KEY) val secretAccessKey: String
)

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
data class DataLake(
        @JsonProperty(NAME) val name: String,
        @JsonProperty(URL) val url: String,
        @JsonProperty(DRIVER) val driver: String,
        @JsonProperty(DATA_FORMAT) val dataFormat: String = driver,
        @JsonProperty(USERNAME) val username: String = "",
        @JsonProperty(PASSWORD) val password: String = "",
        @JsonProperty(HEADER) val header: Boolean = false,
        @JsonProperty(FETCH_SIZE) val fetchSize: Int = DEFAULT_DATA_CHUNK_SIZE,
        @JsonProperty(BATCH_SIZE) val batchSize: Int = DEFAULT_DATA_CHUNK_SIZE,
        @JsonProperty(WRITE_MODE) val writeMode: SaveMode = DEFAULT_WRITE_MODE,
        @JsonProperty(LATTICE_LOGGER) val latticeLogger: Boolean = false,
        @JsonProperty(PROPERTIES) val properties: Properties = Properties()
) {
    companion object  {
        private val logger = LoggerFactory.getLogger(DataLake::class.java)

        @JvmStatic
        fun withUsernameAndPassword( dataLake: DataLake, username: String?, password: String? ): DataLake {
            return DataLake(
                    dataLake.name,
                    dataLake.url,
                    dataLake.driver,
                    dataLake.dataFormat,
                    username?:dataLake.username,
                    password?:dataLake.password,
                    dataLake.header,
                    dataLake.fetchSize,
                    dataLake.batchSize,
                    dataLake.writeMode,
                    dataLake.latticeLogger,
                    dataLake.properties
            )
        }
    }

    init {
        Preconditions.checkState(StringUtils.isNotBlank(name), "Name must not be blank.")
        logger.debug("Created data lake with driver: $driver, using $dataFormat format")
        when( driver ) {
            FILESYSTEM_DRIVER, S3_DRIVER  -> {}
            else -> {
                Preconditions.checkState(StringUtils.isNotBlank(username), "Username cannot be blank for database connections.")
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
