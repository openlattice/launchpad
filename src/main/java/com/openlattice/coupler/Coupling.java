package com.openlattice.coupler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
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
public class Coupling {

    private static final ObjectMapper mapper       = createYamlMapper();
    private static final SparkSession sparkSession = SparkSession.builder()
            .master( "local[" + Runtime.getRuntime().availableProcessors() + "]" )
            .appName( "integration" )
                .getOrCreate();


    public static void main( String[] args ) throws ParseException, IOException {
        CommandLine cl = CouplingCli.parseCommandLine( args );

        Preconditions.checkArgument( cl.hasOption( CouplingCli.FILE ), "Integration file must be specified!" );

        final String integrationFilePath = cl.getOptionValue( CouplingCli.FILE );
        Preconditions.checkState( StringUtils.isNotBlank( integrationFilePath ) );
        File integrationFile = new File( integrationFilePath );

        Integration[] integrations = mapper.readValue( integrationFile, Integration[].class );

        for( Integration integration : integrations ) {
            Dataset<Row> csv = sparkSession.read().csv( "" );
            Dataset<Row> ds = sparkSession
                    .read()
                    .format( "jdbc" )
                    .option( "url", integration.getUrl() )
                    .option( "dbtable", integration.getSql() )
                    .option( "password", integration.getPassword() )
                    .option( "user", integration.getUser() )
                    .option( "driver", integration.getDriver() )
                    .option( "fetchSize", integration.getFetchSize() )
                    .load();

            ds.write()
                    .option( "batchsize", 20000 )
                    .option( "driver", integration.getWriteDriver() )
                    .mode( SaveMode.Overwrite )
                    .jdbc( integration.getWriteUrl(), integration.getWriteTable() , integration.getProperties() );
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
