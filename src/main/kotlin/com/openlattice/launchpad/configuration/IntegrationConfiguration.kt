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

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.collect.ListMultimap
import com.google.common.collect.Sets
import java.util.*

private const val NAME              = "name"
private const val DATASOURCES       = "datasources"
private const val DESTINATIONS      = "destinations"
private const val DESCRIPTION       = "description"
private const val INTEGRATIONS      = "integrations"
private const val AWS_CLIENT        = "awsClient"
private const val SOURCE            = "source"
private const val DESTINATION       = "destination"

const val LEGACY_CSV_FORMAT         = "com.openlattice.launchpad.csv"
const val CSV_FORMAT                = "csv"
const val ORC_FORMAT                = "orc"
const val FILESYSTEM_DRIVER         = "filesystem"
const val S3_DRIVER                 = "s3"

val NON_JDBC_DRIVERS: Set<String>   = Sets.newHashSet(S3_DRIVER, FILESYSTEM_DRIVER)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class IntegrationConfiguration(
        @JsonProperty(NAME) val name: String,
        @JsonProperty(DESCRIPTION) val description: String,
        @JsonProperty(AWS_CLIENT) val awsConfig: Optional<AwsS3ClientConfiguration>,
        @JsonProperty(DATASOURCES) val datasources: List<LaunchpadDatasource>,
        @JsonProperty(DESTINATIONS) val destinations: List<LaunchpadDestination>,
        @JsonProperty(INTEGRATIONS) val integrations: Map<String, ListMultimap<String, Integration>>
)

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

