package com.openlattice.launchpad;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.base.Preconditions;
import com.openlattice.launchpad.configuration.IntegrationConfiguration;
import com.openlattice.launchpad.configuration.IntegrationRunner;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Main class for running launchpad.
 */
@SuppressFBWarnings(value = "SECPTI", justification = "User input for file is considered trusted.")
public class LaunchPad {
    public static final  String       CSV_DRIVER   = "com.openlattice.launchpad.Csv";
    public static final  String       ORC_DRIVER   = "orc";
    private static final ObjectMapper mapper       = createYamlMapper();


    private static final Logger logger = LoggerFactory.getLogger( LaunchPad.class );

    public static void main( String[] args ) throws ParseException, IOException {
        CommandLine cl = LaunchPadCli.parseCommandLine( args );

        Preconditions.checkArgument( cl.hasOption( LaunchPadCli.FILE ), "Integration file must be specified!" );

        final String integrationFilePath = cl.getOptionValue( LaunchPadCli.FILE );
        Preconditions.checkState( StringUtils.isNotBlank( integrationFilePath ) );
        File integrationFile = new File( integrationFilePath );

        IntegrationConfiguration integrationConfiguration = mapper
                .readValue( integrationFile, IntegrationConfiguration.class );
        IntegrationRunner.runIntegrations( integrationConfiguration );
    }

    protected static ObjectMapper createYamlMapper() {
        ObjectMapper yamlMapper = new ObjectMapper( new YAMLFactory() );
        yamlMapper.registerModule( new Jdk8Module() );
        yamlMapper.registerModule( new GuavaModule() );
        yamlMapper.registerModule( new AfterburnerModule() );
        return yamlMapper;
    }
}


