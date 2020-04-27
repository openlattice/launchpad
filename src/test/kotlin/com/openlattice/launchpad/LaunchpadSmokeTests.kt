package com.openlattice.launchpad

import com.openlattice.launchpad.configuration.IntegrationRunner
import org.junit.Before
import org.junit.Test
import java.io.IOException

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class LaunchpadSmokeTests {

    @Before
    fun setup() {
        //connect to db and create
    }

    @Test
    @Throws(IOException::class)
    fun runJdbcJdbcIntegration() {
        IntegrationRunner.runIntegrations( IntegrationConfigLoader.fromJdbc.toJdbc.implicitFormat() )
        // TODO: validate results
    }

    @Test
    @Throws(IOException::class)
    fun runJdbcFsOrcIntegration() {
        IntegrationRunner.runIntegrations( IntegrationConfigLoader.fromJdbc.toFs.orcFormat() )
        // TODO: validate results
    }

    @Test
    @Throws(IOException::class)
    fun runJdbcFsCsvIntegration() {
        IntegrationRunner.runIntegrations( IntegrationConfigLoader.fromJdbc.toFs.csvFormat() )
        // TODO: validate results
    }

    @Test
    @Throws(IOException::class)
    fun runJdbcS3CsvIntegration() {
        IntegrationRunner.runIntegrations( IntegrationConfigLoader.fromJdbc.toS3.csvFormat() )
    }

    @Test
    @Throws(IOException::class)
    fun runJdbcS3OrcIntegration() {
        IntegrationRunner.runIntegrations( IntegrationConfigLoader.fromJdbc.toS3.orcFormat() )
    }
}