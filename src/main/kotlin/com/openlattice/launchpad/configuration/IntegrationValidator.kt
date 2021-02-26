package com.openlattice.launchpad.configuration

import com.amazonaws.services.s3.model.Region
import com.openlattice.launchpad.configuration.Constants.FILESYSTEM_DRIVER
import com.openlattice.launchpad.configuration.Constants.POSTGRES_DRIVER
import com.openlattice.launchpad.configuration.Constants.S3_DRIVER
import com.zaxxer.hikari.pool.HikariPool
import java.sql.SQLException
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class IntegrationValidator private constructor(
        private val validateFunc: (config: IntegrationConfiguration) -> Pair<Boolean, Set<String>>
): Validator<IntegrationConfiguration> {

    companion object {
        val RootValidator = IntegrationValidator { config ->
            val (dlState, dlErrors) = DataLakesValidator.validate(config)
            if (!dlState) {
                return@IntegrationValidator dlState to dlErrors
            }

            val driverResults = config.datalakes.get().map { datalake ->
                val allDriverValidators = driverValidators.getValue(datalake.driver).map {
                    it.validate(config)
                }
                val allState = allDriverValidators.all { it.first }
                val allLogs = allDriverValidators.flatMap { it.second }
                allState to allLogs
            }

            val genericResults = GenericValidations.validate(config)
            val all = (driverResults + genericResults)
            val compositeState = all.all { it.first }
            val errors = all.flatMapTo(mutableSetOf()) { it.second }
            return@IntegrationValidator compositeState to errors
        }

        private val PostgresValidator = IntegrationValidator { config ->
            val jdbcRegex = Regex("""([\w:]+)://([\w_.]*):(\d+)/(\w+)""")
            val pgDlChecks = config.datalakes.get().filter { POSTGRES_DRIVER == it.driver }.map { lake ->
                var state = true
                val logs = mutableSetOf<String>()
                // network checks
                try {
                    lake.getHikariDatasource().connection.use { conn ->
                        conn.createStatement().use { stmt ->
                            config.integrations[lake.name]?.forEach { _, config ->
                                try {
                                    stmt.executeQuery("SELECT * FROM ${config.source}").use {}
                                } catch (ex: SQLException) {
                                    state = false
                                    logs.add("Unable to read from the source table for datalake named `${lake.name}`. Check database access rights/user credentials")
                                }
                            }
                            config.integrations.forEach { (_, dstToConfig) ->
                                val maybeDst = dstToConfig[lake.name]
                                if (maybeDst != null && maybeDst.isNotEmpty()) {
                                    try {
                                        val testTable = UUID.randomUUID()
                                        stmt.execute("CREATE TABLE \"$testTable\"()")
                                        stmt.execute("DROP TABLE \"$testTable\"")
                                    } catch (ex: Exception) {
                                        state = false
                                        logs.add("Unable to create tables in destination datalake named `${lake.name}`. Check database access rights/user credentials")
                                    }
                                }
                            }
                        }
                    }
                } catch (ex: HikariPool.PoolInitializationException) {
                    state = false
                    val match = jdbcRegex.matchEntire(lake.url)
                    if ( match == null ) {
                        logs.add("Unable to connect to datalake ${lake.name} because the JDBC URL is invalid. " +
                                "The expeced format is as follows: `jdbc:postgresql://host:port/database?properties`. Please correct the url string and try again")
                        if (!lake.url.startsWith("jdbc:postgresql://")) {
                            logs.add("The supplied JDBC URL for datalake named `${lake.name}` is invalid. JDBC URLs must start with `jdbc:postgresql://`")
                        }
                    } else {
                        val remotePrefix = match.groupValues[1]
                        val remoteHostname = match.groupValues[2]
                        val remotePort = match.groupValues[3].toInt()
                        val remoteDbname = match.groupValues[4]
                        logs.add("Unable to connect to datalake named `${lake.name}`. Check network connectivity to $remoteHostname " +
                                "on port $remotePort and ensure that database $remoteDbname is configured correctly")
                    }
                }
                state to logs
            }

            return@IntegrationValidator pgDlChecks.all { it.first } to pgDlChecks.flatMapTo(mutableSetOf()) { it.second }
        }

        private val S3Validator = IntegrationValidator { config ->
            val s3DlChecks = config.datalakes.get().filter { S3_DRIVER == it.driver }.map {
                var state = true
                val logs = mutableSetOf<String>()
                if (!it.url.startsWith("s3a://")) {
                    state = false
                    logs.add("The supplied S3 URL for datalake `${it.name} is invalid. S3 URLs must start with `s3a://`")
                }
                state to logs
            }
            return@IntegrationValidator s3DlChecks.all { it.first } to s3DlChecks.flatMapTo(mutableSetOf()) { it.second }
        }

        private val FilesystemValidator = IntegrationValidator { _ ->
            return@IntegrationValidator true to setOf()
        }

        private val GenericValidations = IntegrationValidator { config ->
            val (dlState, dlErrors) = DataLakesValidator.validate(config)
            if (!dlState){
                return@IntegrationValidator dlState to dlErrors
            }
            val lakeNames = config.datalakes.get().mapTo(mutableSetOf()) { it.name }
            val keysErrors = config.integrations.keys.map { dlName ->
                if (!lakeNames.contains(dlName)) {
                    return@map false to setOf("Integration source data lake named $dlName was not found in the list of data lakes. " +
                            "Please specify at least two unique data lakes in the `datalakes:` block to create an integration")
                }
                return@map true to setOf<String>()
            }

            val otherErrors = config.integrations.values.flatMap { otherNameToIntegration ->
                otherNameToIntegration.keys().map { otherDlName ->
                    if (!lakeNames.contains(otherDlName)) {
                        return@map false to setOf("Integration destination data lake named $otherDlName was not found in the list of data lakes. " +
                                "Please specify at least two unique data lakes in the `datalakes:` block to create an integration")
                    }
                    return@map true to setOf<String>()
                }
            }

            val all = (keysErrors + otherErrors)

            return@IntegrationValidator all.all { it.first } to all.flatMapTo(mutableSetOf()) { it.second }
        }

        private val AwsConfigValidator = IntegrationValidator { config ->
            val maybeAws = config.awsConfig
            if (maybeAws.isEmpty) {
                return@IntegrationValidator false to setOf("No AWS configuration was specified but one was expected. " +
                        "Please at least specify the `awsConfig: ` block with the `regionName` property " +
                        "and an AWS secretAccessKey and accesKeyId specified as environment variables")
            }
            val awsRegion = maybeAws.get().regionName
            if (awsRegion.isEmpty()) {
                return@IntegrationValidator false to setOf("The AWS region was not specified in the integration config file")
            }
            try {
                Region.fromValue(awsRegion)
                return@IntegrationValidator true to setOf()
            } catch (ex: Exception) {
                return@IntegrationValidator false to setOf("The AWS region specified in the integration config file " +
                        "(`$awsRegion`) is not a valid AWS region")
            }
        }

        private val DataLakesValidator = IntegrationValidator {
            val maybeLakes = it.datalakes
            if (maybeLakes.isEmpty) {
                return@IntegrationValidator false to setOf("There are no data lakes specified. " +
                        "Please specify at least two data lakes to transfer data between")
            }
            return@IntegrationValidator true to setOf()
        }

        private val driverValidators = mapOf(
                POSTGRES_DRIVER to listOf(PostgresValidator),
                FILESYSTEM_DRIVER to listOf(FilesystemValidator),
                S3_DRIVER to listOf(AwsConfigValidator, S3Validator)
        )
    }

    @Transient
    private var cachedValidationResult: Pair<Boolean, Set<String>>? = null

    override fun validate(config: IntegrationConfiguration): Pair<Boolean, Set<String>> {
        if (cachedValidationResult == null ){
            cachedValidationResult = validateFunc(config)
        }
        return cachedValidationResult!!
    }
}

interface Validator<C> {
    fun validate(config: C): Pair<Boolean, Set<String>>
}