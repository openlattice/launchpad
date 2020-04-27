package com.openlattice.launchpad

import com.google.common.io.Resources
import com.openlattice.launchpad.configuration.IntegrationConfiguration
import org.junit.Assert
import java.io.IOException
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class IntegrationConfigLoader: AbstractJacksonSerializationTest<IntegrationConfiguration>() {

    override fun getSampleData(): IntegrationConfiguration {
        try {
            return asYaml("integrations_serialization_test.yaml")
        } catch (e: IOException) {
            e.printStackTrace()
            Assert.fail("IOException getting sample data ")
            return IntegrationConfiguration( "", "", Optional.empty(), listOf(), listOf(), mapOf())
        }
    }

    override fun getClazz(): Class<IntegrationConfiguration> {
        return IntegrationConfiguration::class.java
    }

    companion object {
        @Throws(IOException::class)
        fun asYaml( filename: String ): IntegrationConfiguration {
            return yaml.readValue(Resources.getResource(filename), IntegrationConfiguration::class.java)!!
        }
    }

    object fromJdbc {
        object toJdbc {
            @Throws(IOException::class)
            fun implicitFormat(): IntegrationConfiguration {
                return asYaml("integrations_jdbc_jdbc.yaml")
            }
        }

        object toFs {
            @Throws(IOException::class)
            fun orcFormat(): IntegrationConfiguration {
                return asYaml("integrations_jdbc_fs_orc.yaml")
            }

            @Throws(IOException::class)
            fun csvFormat(): IntegrationConfiguration {
                return asYaml("integrations_jdbc_fs_csv.yaml")
            }

            @Throws(IOException::class)
            fun parquetFormat(): IntegrationConfiguration {
                return asYaml("integrations_jdbc_fs_parquet.yaml")
            }
        }

        object toS3 {
            @Throws(IOException::class)
            fun orcFormat(): IntegrationConfiguration {
                return asYaml("integrations_jdbc_s3_orc.yaml")
            }

            @Throws(IOException::class)
            fun csvFormat(): IntegrationConfiguration {
                return asYaml("integrations_jdbc_s3_csv.yaml")
            }

            @Throws(IOException::class)
            fun parquetFormat(): IntegrationConfiguration {
                return asYaml("integrations_jdbc_s3_parquet.yaml")
            }
        }
    }
}