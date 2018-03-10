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

package com.openlattice.launchpad.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Configuration class for running integrations.
 */
public class IntegrationConfiguration {
    private static final String USERNAME     = "name";
    private static final String PASSWORD     = "description";
    private static final String INTEGRATIONS = "integrations";

    private final String            name;
    private final String            description;
    private final List<Integration> integrations;

    @JsonCreator
    public IntegrationConfiguration(
            @JsonProperty( USERNAME ) String name,
            @JsonProperty( PASSWORD ) String description,
            @JsonProperty( INTEGRATIONS ) List<Integration> integrations ) {
        this.name = name;
        this.description = description;
        this.integrations = integrations;
    }

    @JsonProperty( USERNAME )
    public String getName() {
        return name;
    }

    @JsonProperty( PASSWORD )
    public String getDescription() {
        return description;
    }

    @JsonProperty( INTEGRATIONS )
    public List<Integration> getIntegrations() {
        return integrations;
    }
}
