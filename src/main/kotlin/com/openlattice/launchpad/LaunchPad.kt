package com.openlattice.launchpad

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.module.afterburner.AfterburnerModule
import com.google.common.base.Preconditions
import com.openlattice.launchpad.configuration.IntegrationConfiguration
import com.openlattice.launchpad.configuration.IntegrationRunner
import org.apache.commons.io.FilenameUtils
import org.slf4j.LoggerFactory
import java.io.File

class LaunchPad {

    companion object {
        val mapper = createYamlMapper()

        val logger = LoggerFactory.getLogger( LaunchPad::class.java)

        fun main( args: Array<String> ) {
            val cli = LaunchPadCli.parseCommandLine( args )

            Preconditions.checkArgument( cli.hasOption( LaunchPadCli.FILE ), "Integration file must be specified!" );


            val integrationFilePath = cli.getOptionValue( LaunchPadCli.FILE )
            Preconditions.checkState(  integrationFilePath.isNullOrEmpty() )
            val integrationFile = File( FilenameUtils.getName( integrationFilePath  ) )

            val integrationConfiguration = mapper.readValue( integrationFile, IntegrationConfiguration::class.java )

            IntegrationRunner.runIntegrations( integrationConfiguration );
        }

        private fun createYamlMapper() : ObjectMapper {
            val yamlMapper = ObjectMapper( YAMLFactory() );
            yamlMapper.registerModule( Jdk8Module() );
            yamlMapper.registerModule( GuavaModule() );
            yamlMapper.registerModule( AfterburnerModule() );
            return yamlMapper;
        }
    }
}