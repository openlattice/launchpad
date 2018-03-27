package com.openlattice.launchpad;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.openlattice.launchpad.configuration.Integration;
import com.openlattice.launchpad.configuration.IntegrationConfiguration;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LaunchPad {
    public static final  String       CSV_DRIVER   = "com.openlattice.launchpad.Csv";
    private static final ObjectMapper mapper       = createYamlMapper();
    private static final SparkSession sparkSession = SparkSession.builder()
            .master( "local[" + Runtime.getRuntime().availableProcessors() + "]" )
            .appName( "integration" )
            .getOrCreate();

    public static void main( String[] args ) throws ParseException, IOException {
        CommandLine cl = LaunchPadCli.parseCommandLine( args );

        Preconditions.checkArgument( cl.hasOption( LaunchPadCli.FILE ), "Integration file must be specified!" );

        final String integrationFilePath = cl.getOptionValue( LaunchPadCli.FILE );
        Preconditions.checkState( StringUtils.isNotBlank( integrationFilePath ) );
        File integrationFile = new File( integrationFilePath );

        IntegrationConfiguration integrationConfiguration = mapper
                .readValue( integrationFile, IntegrationConfiguration.class );
        runIntegrations( integrationConfiguration );
    }

    @VisibleForTesting
    public static void runIntegrations( IntegrationConfiguration integrationConfiguration ) {
        List<Integration> integrations = integrationConfiguration.getIntegrations();

        for ( Integration integration : integrations ) {
            Dataset<Row> ds = getSourceDataset( integration );
            //Only CSV and JDBC are tested.
            switch ( integration.getDestination().getWriteDriver() ) {
                case CSV_DRIVER:
                    ds.write().option( "header", true ).csv( integration.getDestination().getWriteUrl() );
                    break;
                case "parquet":
                    ds.write().parquet( integration.getDestination().getWriteUrl() );
                    break;
                case "orc":
                    ds.write().orc( integration.getDestination().getWriteUrl() );
                    break;
                default:
                    ds.write()
                            .option( "batchsize", integration.getDestination().getBatchSize() )
                            .option( "driver", integration.getDestination().getWriteDriver() )
                            .mode( SaveMode.Overwrite )
                            .jdbc( integration.getDestination().getWriteUrl(),
                                    integration.getDestination().getWriteTable(),
                                    integration.getDestination().getProperties() );
            }
        }
    }

    protected static Dataset<Row> getSourceDataset( Integration integration ) {
        switch ( integration.getSource().getDriver() ) {
            case CSV_DRIVER:
                return sparkSession
                        .read()
                        .option( "header", true )
                        .option( "inferSchema", true )
                        .csv( integration.getSource().getUrl() );
            default:
                return sparkSession
                        .read()
                        .format( "jdbc" )
                        .option( "url", integration.getSource().getUrl() )
                        .option( "dbtable", integration.getSource().getSql() )
                        .option( "user", integration.getSource().getUser() )
                        .option( "password", integration.getSource().getPassword() )
                        .option( "driver", integration.getSource().getDriver() )
                        .option( "fetchSize", integration.getSource().getFetchSize() )
                        .load();
        }
    }

    protected static ObjectMapper createYamlMapper() {
        ObjectMapper yamlMapper = new ObjectMapper( new YAMLFactory() );
        yamlMapper.registerModule( new Jdk8Module() );
        yamlMapper.registerModule( new GuavaModule() );
        yamlMapper.registerModule( new AfterburnerModule() );
        return yamlMapper;
    }
}
