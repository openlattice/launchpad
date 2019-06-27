package com.openlattice.launchpad.configuration

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.logging.log4j.core.tools.picocli.CommandLine
import java.util.*

private const val DEFAULT_FETCH_SIZE = 20000


data class LaunchpadDatasource(
        @JsonProperty(NAME) val name: String,
        @JsonProperty(URL) val url: String,
        @JsonProperty(DRIVER) val driver: String,
        @JsonProperty(USER) val user: String = "",
        @JsonProperty(PASSWORD) val password: String = "",
        @JsonProperty(FETCH_SIZE) val fetchSize: Int = DEFAULT_FETCH_SIZE,
        @JsonProperty(HEADER) val header: Boolean = false ) {

    val properties: Properties = Properties()

    init {
        if ( driver != CSV_DRIVER && user.isEmpty() ){
            throw CommandLine.MissingParameterException("A username must be specified for database connections.")
        }
        properties.setProperty( "user", user );
        properties.setProperty( "password", password );
        properties.setProperty( "driver", driver );
    }
}