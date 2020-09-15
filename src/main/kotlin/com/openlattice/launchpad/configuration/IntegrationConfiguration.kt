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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.fasterxml.jackson.annotation.JsonFilter
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Preconditions
import com.google.common.collect.ListMultimap
import com.openlattice.launchpad.configuration.Constants.ACCESS_KEY_ID
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
import com.openlattice.launchpad.configuration.Constants.JDBC_URL
import com.openlattice.launchpad.configuration.Constants.LATTICE_LOGGER
import com.openlattice.launchpad.configuration.Constants.NAME
import com.openlattice.launchpad.configuration.Constants.PASSWORD
import com.openlattice.launchpad.configuration.Constants.PROPERTIES
import com.openlattice.launchpad.configuration.Constants.S3_DRIVER
import com.openlattice.launchpad.configuration.Constants.SECRET_ACCESS_KEY
import com.openlattice.launchpad.configuration.Constants.SOURCE
import com.openlattice.launchpad.configuration.Constants.URL
import com.openlattice.launchpad.configuration.Constants.USER
import com.openlattice.launchpad.configuration.Constants.USERNAME
import com.openlattice.launchpad.configuration.Constants.WRITE_MODE
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.util.*

private const val DATASOURCES       = "datasources"
private const val DESTINATIONS      = "destinations"
private const val INTEGRATIONS      = "integrations"
private const val AWS_CONFIG        = "awsConfig"
private const val DATA_LAKES        = "datalakes"
private const val GLUTTONY          = "gluttony"
private const val MERGE_SQL         = "mergeSql"
private const val MASTER_TABLE_SQL  = "masterTableSql"

private const val EXTRA_SPARK_PARAMETERS = "sparkParameters"
private const val REGION_NAME       = "regionName"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
data class IntegrationConfiguration(
        @JsonProperty(NAME) val name: String,
        @JsonProperty(DESCRIPTION) val description: String,
        @JsonProperty(AWS_CONFIG) val awsConfig: Optional<AwsS3ClientConfiguration>,
        @JsonProperty(EXTRA_SPARK_PARAMETERS) val extraSparkParameters: Optional<Map<String, String>>,
        @JsonProperty(DATASOURCES) val datasources: Optional<List<LaunchpadDatasource>>,
        @JsonProperty(DESTINATIONS) val destinations: Optional<List<LaunchpadDestination>>,
        @JsonProperty(DATA_LAKES) val datalakes: Optional<List<DataLake>>,
        @JsonProperty(INTEGRATIONS) val integrations: Map<String, ListMultimap<String, Integration>> = mapOf(),
        @JsonProperty(ARCHIVES) val archives: Map<String, Map<String, List<Archive>>> = mapOf()
) {
    init {
        Preconditions.checkState( ( datasources.isPresent && destinations.isPresent ) || datalakes.isPresent,
        "Must specify either one or more datasources and destinations or one or more data lakes")
    }

    fun convertToDataLakesIfPresent(): Map<String, DataLake> {
        val sourcesConfig = datasources.orElse(listOf())
        val destsConfig = destinations.orElse(listOf())

        // map to lakes if needed. This will be removed once launchpads are upgraded
        return datalakes.orElseGet {
            val newLakes = ArrayList<DataLake>(destsConfig.size + sourcesConfig.size)
            destsConfig.forEach { newLakes.add(it.asDataLake()) }
            sourcesConfig.forEach { newLakes.add(it.asDataLake()) }
            newLakes
        }.associateBy { it.name }
    }
}

/**
 * @param description An optional description parameter for documenting the integration.
 * @param source The source table or SQL which determines what data will be transferred.
 * @param destination The destination table where data will be written.
 * @param mergeSql Optional parameter for specifying the UPSERT SQL behavior for merging into the master table if a
 * conflict is encountered.
 * @param masterTableSql Optional parameter specifying the creation SQL for master table into which updates should be
 * upserted. For safety, use CREATE TABLE IF NOT EXISTS so that call doesn't fail it table already exists.
 * @param gluttony Setting gluttony to true will attempt to use the global catalog to ingest all accessible tables
 * in the database.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
data class Integration(
        @JsonProperty(DESCRIPTION) val description : String = "",
        @JsonProperty(SOURCE) val source: String,
        @JsonProperty(DESTINATION) val destination: String,
        @JsonProperty(MERGE_SQL) val mergeSql: String = "",
        @JsonProperty(MASTER_TABLE_SQL) val masterTableSql : String = "",
        @JsonProperty(GLUTTONY) val gluttony : Boolean = false
): Transferable {
    @JsonIgnore
    override fun getLaunchPad(): String {
        return source
    }

    @JsonIgnore
    override fun getLandingPad(): String {
        return destination
    }

    @JsonIgnore
    override fun getSourceName(): String {
        return source
    }

    @JsonIgnore
    override fun getBucketColumn(): String? {
        return null
    }
}

interface Transferable {
    @JsonIgnore
    fun getSourceName(): String

    @JsonIgnore
    fun getLandingPad(): String

    @JsonIgnore
    fun getLaunchPad(): String

    @JsonIgnore
    fun getBucketColumn(): String?
}

const val ARCHIVES = "archives"
const val STRATEGY = "strategy"
const val CONSTRAINTS = "constraints"
const val BUCKET_COLUMN = "column"
const val INTERVAL_COUNT = "intervalCount"
const val INTERVAL_UNIT = "intervalUnit"

data class Archive(
        @JsonProperty(SOURCE) val source: String,
        @JsonProperty(STRATEGY) val strategy: DailyBucketingArchiveStrategy,
        @JsonProperty(DESCRIPTION) val description : String = "",
        @JsonProperty(DESTINATION) val destination: String
): Transferable {
    @JsonIgnore
    override fun getLaunchPad(): String {
        return getArchiveSql()
    }

    @JsonIgnore
    override fun getLandingPad(): String {
        return destination
    }

    @JsonIgnore
    override fun getSourceName(): String {
        return source
    }

    @JsonIgnore
    override fun getBucketColumn(): String? {
        return strategy.column
    }

    fun getArchiveSql(): String {
        return strategy.getArchiveSql(source)
    }
}

data class DailyBucketingArchiveStrategy(
        @JsonProperty(BUCKET_COLUMN) var column: String,
        @JsonProperty(CONSTRAINTS) var constraints: List<String> = listOf()
) {
    fun getArchiveSql( table: String ): String {
        val constraintsClause = if (constraints.isNotEmpty()) {
            "WHERE ${constraints.joinToString(" AND ")}"
        } else { "" }

        return "(SELECT *, $column::date as bucket_val FROM $table $constraintsClause) dh"
    }
}

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@JsonFilter(Constants.CREDENTIALS_FILTER)
data class AwsS3ClientConfiguration(
        @JsonProperty(REGION_NAME) val regionName: String,
        @JsonProperty(ACCESS_KEY_ID) val accessKeyId: String,
        @JsonProperty(SECRET_ACCESS_KEY) val secretAccessKey: String
)

enum class DataFormat( val extension: String ) {
    CSV_FORMAT(".csv"),
    ORC_FORMAT(".orc"),
    LEGACY_CSV_FORMAT(".csv")
}

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@JsonFilter(Constants.CREDENTIALS_FILTER)
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
    }

    init {
        Preconditions.checkState(StringUtils.isNotBlank(name), "Name must not be blank.")
        logger.debug("Created data lake with driver: $driver, using $dataFormat format")
        when( driver ) {
            FILESYSTEM_DRIVER, S3_DRIVER -> {}
            else -> {
                Preconditions.checkState(StringUtils.isNotBlank(username), "Username cannot be blank for database connections.")
                if ( password == "" ){
                    logger.warn("Connecting to $name with a blank password.")
                }
                if ( properties.isEmpty ){
                    properties.put(JDBC_URL, url)
                    properties.put(Constants.MAXIMUM_POOL_SIZE, "1")
                    properties.put(Constants.CONNECTION_TIMEOUT, "120000") //2-minute connection timeout
                    properties.put(USER, username)
                    properties.put(USERNAME, username)
                    properties.put(PASSWORD, password)
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

fun configureOrGetSparkSession( integrationConfiguration: IntegrationConfiguration ): SparkSession {
    val session = SparkSession.builder()
            .master("local[${Runtime.getRuntime().availableProcessors()}]")
            .appName("integration")
    if ( integrationConfiguration.extraSparkParameters.isPresent ) {
        integrationConfiguration.extraSparkParameters.get().forEach { (k, v) ->
            session.config(k, v )
        }
    }
    if ( integrationConfiguration.awsConfig.isPresent ) {
        val config = DefaultAWSCredentialsProviderChain().credentials
        val manualConfig = integrationConfiguration.awsConfig.get()
        session
                .config("fs.s3a.access.key", config.awsAccessKeyId)
                .config("fs.s3a.secret.key", config.awsSecretKey)
                .config("fs.s3a.endpoint", "s3.${manualConfig.regionName}.amazonaws.com")
                .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
                .config("spark.hadoop.fs.s3a.fast.upload", "true")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .config("spark.speculation", "false")
    }
    return session.orCreate
}
