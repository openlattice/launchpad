package com.openlattice.launchpad.serialization

import com.openlattice.launchpad.AbstractJacksonSerializationTest
import com.openlattice.launchpad.IntegrationConfigLoader
import com.openlattice.launchpad.configuration.AwsS3ClientConfiguration
import com.openlattice.launchpad.configuration.DataLake
import com.openlattice.launchpad.configuration.Integration
import com.openlattice.launchpad.configuration.IntegrationConfiguration
import com.openlattice.launchpad.configuration.LaunchpadDatasource
import com.openlattice.launchpad.configuration.LaunchpadDestination
import org.junit.Assert
import java.io.IOException
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class IntegrationConfigurationSerializationTest : AbstractJacksonSerializationTest<IntegrationConfiguration>() {
    override fun getSampleData(): IntegrationConfiguration {
        return try {
            IntegrationConfigLoader.asYaml("integrations_serialization_test.yaml")
        } catch (e: IOException) {
            e.printStackTrace()
            Assert.fail("IOException getting sample data ")
            IntegrationConfiguration( "", "",
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    mapOf(),
                    mapOf())
        }
    }

    override fun getClazz(): Class<IntegrationConfiguration> {
        return IntegrationConfiguration::class.java
    }
}

class IntegrationSerializationTest : AbstractJacksonSerializationTest<Integration>() {
    override fun getSampleData(): Integration {
        try {
            return IntegrationConfigLoader.asYaml("integrations_serialization_test.yaml")
                    .integrations.values.iterator().next()
                    .values().iterator().next()
        } catch (e: IOException) {
            e.printStackTrace()
            Assert.fail("IOException getting sample data ")
            return Integration( "", "", "", gluttony = false)
        }
    }

    override fun getClazz(): Class<Integration> {
        return Integration::class.java
    }
}

class DataLakeSerializationTest : AbstractJacksonSerializationTest<DataLake>() {
    override fun getSampleData(): DataLake {
        try {
            return IntegrationConfigLoader.asYaml("integrations_serialization_test.yaml").datalakes.get().first()
        } catch (e: IOException) {
            e.printStackTrace()
            Assert.fail("IOException getting sample data ")
            return DataLake("F", "url", "csv")
        }
    }

    override fun getClazz(): Class<DataLake> {
        return DataLake::class.java
    }
}

class AwsConfigSerializationTest: AbstractJacksonSerializationTest<AwsS3ClientConfiguration>() {
    override fun getSampleData(): AwsS3ClientConfiguration {
        try {
            return IntegrationConfigLoader.asYaml("integrations_serialization_test.yaml").awsConfig.get()
        } catch (e: IOException) {
            e.printStackTrace()
            Assert.fail("IOException getting sample data ")
            return AwsS3ClientConfiguration("", "", "")
        }
    }

    override fun getClazz(): Class<AwsS3ClientConfiguration> {
        return AwsS3ClientConfiguration::class.java
    }
}

class LaunchpadDestinationSerializationTest: AbstractJacksonSerializationTest<LaunchpadDestination>() {
    override fun getSampleData():LaunchpadDestination {
        try {
            return IntegrationConfigLoader.asYaml("integrations_serialization_test.yaml").destinations.get().first()
        } catch (e: IOException) {
            e.printStackTrace()
            Assert.fail("IOException getting sample data ")
            return LaunchpadDestination("", "", "", Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
        }
    }

    override fun getClazz(): Class<LaunchpadDestination> {
        return LaunchpadDestination::class.java
    }
}

class LaunchpadDatasourceSerializationTest: AbstractJacksonSerializationTest<LaunchpadDatasource>() {
    override fun getSampleData(): LaunchpadDatasource {
        try {
            return IntegrationConfigLoader.asYaml("integrations_serialization_test.yaml").datasources.get().first()
        } catch (e: IOException) {
            e.printStackTrace()
            Assert.fail("IOException getting sample data ")
            return LaunchpadDatasource("", "", "", Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
        }
    }

    override fun getClazz(): Class<LaunchpadDatasource > {
        return LaunchpadDatasource ::class.java
    }
}
