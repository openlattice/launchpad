package com.openlattice.launchpad.configuration;

import com.google.common.io.Resources;
import com.openlattice.launchpad.AbstractJacksonSerializationTest;

import java.io.IOException;

public class AppendIntegrationConfigurationTests extends AbstractJacksonSerializationTest<IntegrationConfiguration> {

    public static IntegrationConfiguration readAppendOnlyIntegrationConfiguration() throws IOException {
        return yaml.readValue( Resources.getResource( "appendIntegrations.yaml" ), IntegrationConfiguration.class );
    }

    @Override protected IntegrationConfiguration getSampleData() {
        try {
            return readAppendOnlyIntegrationConfiguration();
        } catch ( IOException e ) {
            return null;
        }
    }

    @Override protected Class<IntegrationConfiguration> getClazz() {
        return IntegrationConfiguration.class;
    }
}
