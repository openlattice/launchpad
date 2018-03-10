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

package com.openlattice.coupler;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Configuration class for running integrations.
 */
public class IntegrationConfiguration {
    private static final String USERNAME     = "olUsername";
    private static final String PASSWORD     = "olPassword";
    private static final String INTEGRATIONS = "integrations";
    private final String            olUsername;
    private final String            olPassword;
    private final List<Integration> integrations;

    @JsonCreator
    public IntegrationConfiguration(
            @JsonProperty( USERNAME ) String olUsername,
            @JsonProperty( PASSWORD ) String olPassword,
            @JsonProperty( INTEGRATIONS ) List<Integration> integrations ) {
        this.olUsername = olUsername;
        this.olPassword = olPassword;
        this.integrations = integrations;
    }

    @JsonProperty( USERNAME )
    public String getOlUsername() {
        return olUsername;
    }

    @JsonProperty( PASSWORD )
    public String getOlPassword() {
        return olPassword;
    }

    @JsonProperty( INTEGRATIONS )
    public List<Integration> getIntegrations() {
        return integrations;
    }
}
