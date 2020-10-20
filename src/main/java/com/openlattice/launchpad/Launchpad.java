package com.openlattice.launchpad;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.openlattice.launchpad.configuration.*;
import com.openlattice.launchpad.serialization.JacksonSerializationConfiguration;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.configureOrGetSparkSession;

/**
 * Main class for running launchpad.
 */
@SuppressFBWarnings(value = "SECPTI", justification = "User input for file is considered trusted.")
public class Launchpad {
    private static final ObjectMapper mapper            = JacksonSerializationConfiguration.yamlMapper;

    private static final Logger logger = LoggerFactory.getLogger( Launchpad.class );

    public static void main( String[] args ) throws IOException {
        CommandLine cl = LaunchpadCli.parseCommandLine( args );

        if ( cl.hasOption( LaunchpadCli.HELP )){
            LaunchpadCli.printHelp();
            System.exit(0);
        }

        Preconditions.checkArgument( cl.hasOption( LaunchpadCli.FILE ), "Integration file must be specified!" );

        final String integrationFilePath = cl.getOptionValue( LaunchpadCli.FILE );
        Preconditions.checkState( StringUtils.isNotBlank( integrationFilePath ) );
        File integrationFile = new File( integrationFilePath );

        IntegrationConfiguration config = null;
        try {
             config = mapper.readValue(
                    integrationFile,
                    IntegrationConfiguration.class );
        } catch ( Exception ex ) {
            System.out.println("There was an error parsing your integration configuration file. Please check your file and run launchpad again");
            ex.printStackTrace();
            System.exit(-1);
        }

        Optional<List<DataLake>> currentLakes = config.getDatalakes();
        if ( !currentLakes.isPresent() || currentLakes.get().isEmpty() ) {
            IntegrationConfiguration newConfig = convertToDataLakes( config );
            String newJson = mapper.writeValueAsString( newConfig );
            System.out.println("Please replace your current yaml configuration file with the below yaml:");
            System.out.println(newJson);

            System.exit( -1 );
        }

        Map<String, ListMultimap<String, Integration>> integrations = config.getIntegrations();
        Map<String, Map<String, List<Archive>>> archives = config.getArchives();

        boolean runIntegrations;

        if ( integrations.isEmpty() ) {
            if ( archives.isEmpty() ) {
                System.out.println("Either integrations or archives must be specified to run launchpad");
                System.exit( -1 );
            }
            System.out.println("Setting up archive run with launchpad");
            runIntegrations = false;
        } else {
            if ( !archives.isEmpty() ) {
                System.out.println("Only one of [ integrations, archives ] can be specified to run launchpad");
                System.exit( -1 );
            }
            System.out.println("Setting up integration run with launchpad");
            runIntegrations = true;
        }

        try ( SparkSession session = configureOrGetSparkSession( config )) {
            if ( runIntegrations ) {
                IntegrationRunner.runIntegrations( config, session );
            } else {
                ArchiveRunner.runArchives( config, session );
            }
        } catch ( Exception ex ) {
            logger.error( "Exception running launchpad integration", ex);
        }
    }

    public static IntegrationConfiguration convertToDataLakes( IntegrationConfiguration config ) {
        Optional<List<DataLake>> currentLakes = config.getDatalakes();
        if ( !currentLakes.isPresent() || currentLakes.get().isEmpty() ) {
            List<DataLake> lakes = Lists.newArrayList();
            for ( LaunchpadDatasource source : config.getDatasources().get()){
                lakes.add(source.asDataLake());
            }
            for ( LaunchpadDestination dest : config.getDestinations().get()){
                lakes.add(dest.asDataLake());
            }
            return new IntegrationConfiguration(
                    config.getName(),
                    config.getDescription(),
                    config.getAwsConfig(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(lakes),
                    config.getIntegrations(),
                    new HashMap<>()
            );
        }
        return config;
    }
}


