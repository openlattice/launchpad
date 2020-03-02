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

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
const val SOURCE = "source"
const val DESTINATION = "destination"
const val ISOLATION_LEVEL = "transactionIsolationLevel"

data class Integration(
        @JsonProperty(DESCRIPTION) val description : String = "",
        @JsonProperty(SOURCE) val source: String,
        @JsonProperty(DESTINATION) val destination: String,
        @JsonProperty(ISOLATION_LEVEL) val isolationLevel: IsolationLevel = IsolationLevel.READ_UNCOMMITTED,
        @JsonProperty("gluttony") val gluttony : Boolean = false
)

enum class IsolationLevel {
    NONE, READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, SERIALIZABLE
}