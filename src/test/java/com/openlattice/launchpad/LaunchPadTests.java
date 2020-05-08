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

package com.openlattice.launchpad;

import com.openlattice.launchpad.configuration.DataLake;
import com.openlattice.launchpad.configuration.IntegrationConfiguration;
import com.openlattice.launchpad.configuration.IntegrationConfigurationTests;
import com.openlattice.launchpad.configuration.IntegrationRunner;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LaunchPadTests {
    @Test
    public void runAppendOnlyIntegration() throws IOException {
        IntegrationConfiguration integrationConfiguration = IntegrationConfigurationTests
                .readAppendOnlyIntegrationConfiguration();
        List<DataLake> dataLakes = integrationConfiguration.getDatalakes().get();
        for ( DataLake d : dataLakes ) {
            System.out.println( d.getName() + ": "  + d.getWriteMode());
        }
        IntegrationRunner.runIntegrations( integrationConfiguration  );
    }
}
