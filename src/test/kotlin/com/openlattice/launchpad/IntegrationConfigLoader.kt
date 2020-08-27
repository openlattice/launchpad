package com.openlattice.launchpad

import com.google.common.io.Resources
import com.openlattice.launchpad.configuration.IntegrationConfiguration
import com.openlattice.launchpad.serialization.JacksonSerializationConfiguration
import java.io.IOException

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class IntegrationConfigLoader {
    companion object {
        @Throws(IOException::class)
        fun asYaml( filename: String ): IntegrationConfiguration {
                return JacksonSerializationConfiguration.yamlMapper.readValue(Resources.getResource(filename),
                    IntegrationConfiguration::class.java)!!
        }
    }

    object fromCsv {
        object toJdbc {
            @Throws(IOException::class)
            fun implicitFormat(): IntegrationConfiguration {
                return asYaml("integrations_fs_csv_jdbc.yaml")
            }
        }
    }

    object fromJdbc {
        object toOracle {
            @Throws(IOException::class)
            fun implicitFormat(): IntegrationConfiguration {
                return asYaml("integrations_jdbc_oracle.yaml")
            }
        }

        object toJdbc {
            @Throws(IOException::class)
            fun implicitFormat(): IntegrationConfiguration {
                return asYaml("integrations_jdbc_jdbc.yaml")
            }

            @Throws(IOException::class)
            fun appendOnlyConfiguration(): IntegrationConfiguration {
                return asYaml("appendIntegrations.yaml")
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