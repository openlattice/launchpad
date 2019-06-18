
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
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

private const val DEFAULT_BATCH_SIZE = 20000
private val logger = LoggerFactory.getLogger(LaunchpadDestination::class.java)

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class LaunchpadDestination(
        @JsonProperty(NAME) val name: String,
        @JsonProperty(WRITE_URL)  val writeUrl: String,
        @JsonProperty(WRITE_DRIVER)  val writeDriver: String,
        @JsonProperty(USER) val username: Optional<String>,
        @JsonProperty(PASSWORD) val password: Optional<String>,
        @JsonProperty(PROPERTIES) val properties: Properties = Properties(),
        @JsonProperty(BATCH_SIZE) val batchSize: Int = DEFAULT_BATCH_SIZE) {

    init {
        Preconditions.checkState(name.isEmpty(), "Name must be specified for a desintation.")
        username.ifPresent { u -> this.properties.setProperty("user", u) }
        password.ifPresent { p -> this.properties.setProperty(PASSWORD, p) }
    }

    val hikariDatasource: HikariDataSource
        @JsonIgnore
        get() {
            val hc = HikariConfig(properties)
            logger.info("JDBC URL = {}", hc.jdbcUrl)
            return HikariDataSource(hc)
        }


}
