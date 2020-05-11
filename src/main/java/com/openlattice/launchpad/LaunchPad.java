package com.openlattice.launchpad;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.openlattice.launchpad.configuration.DataLake;
import com.openlattice.launchpad.configuration.IntegrationConfiguration;
import com.openlattice.launchpad.configuration.IntegrationRunner;
import com.openlattice.launchpad.configuration.LaunchpadDatasource;
import com.openlattice.launchpad.configuration.LaunchpadDestination;
import com.openlattice.launchpad.serialization.JacksonSerializationConfiguration;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Main class for running launchpad.
 */
@SuppressFBWarnings(value = "SECPTI", justification = "User input for file is considered trusted.")
public class LaunchPad {
    private static final ObjectMapper mapper            = JacksonSerializationConfiguration.yamlMapper;

    private static final Logger logger = LoggerFactory.getLogger( LaunchPad.class );


    public static void main( String[] args ) throws ParseException, IOException {
        CommandLine cl = LaunchPadCli.parseCommandLine( args );

        if ( cl.hasOption( LaunchPadCli.HELP )){
            LaunchPadCli.printHelp();
            System.exit(0);
        }

        Preconditions.checkArgument( cl.hasOption( LaunchPadCli.FILE ), "Integration file must be specified!" );

        final String integrationFilePath = cl.getOptionValue( LaunchPadCli.FILE );
        Preconditions.checkState( StringUtils.isNotBlank( integrationFilePath ) );
        File integrationFile = new File( integrationFilePath );

        IntegrationConfiguration config = mapper.readValue(
                integrationFile,
                IntegrationConfiguration.class );

        Optional<List<DataLake>> currentLakes = config.getDatalakes();
        if ( !currentLakes.isPresent() || ( currentLakes.isPresent() && currentLakes.get().isEmpty() )) {
            List<DataLake> lakes = Lists.newArrayList();
            for ( LaunchpadDatasource source : config.getDatasources().get()){
                lakes.add(source.asDataLake());
            }
            for ( LaunchpadDestination dest : config.getDestinations().get()){
                lakes.add(dest.asDataLake());
            }
            IntegrationConfiguration newConfig = new IntegrationConfiguration(
                config.getName(),
                config.getDescription(),
                config.getAwsConfig(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(lakes),
                config.getIntegrations()
            );

            String newJson = mapper.writeValueAsString( newConfig );
            System.out.println("Please replace your current yaml configuration file with the below yaml:");
            System.out.println(newJson);

            System.exit( -1 );
        }

        IntegrationRunner.runIntegrations( config );
    }
}


