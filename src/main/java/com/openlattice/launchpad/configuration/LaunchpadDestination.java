/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.launchpad.configuration;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.CSV_FORMAT;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.LEGACY_CSV_FORMAT;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.DEFAULT_DATA_CHUNK_SIZE;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.DEFAULT_WRITE_MODE;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.S3_DRIVER;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.FILESYSTEM_DRIVER;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.ORC_FORMAT;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.NON_JDBC_DRIVERS;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Deprecated
public class LaunchpadDestination {
    private static final String NAME                = "name";
    private static final String JDBC_URL            ="jdbcUrl";
    private static final String MAXIMUM_POOL_SIZE   = "maximumPoolSize";
    private static final String CONNECTION_TIMEOUT  = "connectionTimeout";
    private static final String WRITE_URL           = "url";
    private static final String WRITE_DRIVER        = "driver";
    private static final String WRITE_MODE          = "writeMode";
    private static final String USER                = "username";
    private static final String PASSWORD            = "password";
    private static final String DATA_FORMAT         = "dataFormat";
    private static final String PROPERTIES          = "properties";
    private static final String BATCH_SIZE          = "batchSize";

    private static final Logger      logger = LoggerFactory.getLogger( LaunchpadDestination.class );

    private final String        name;
    private final String        username;
    private final String        password;
    private final String        writeUrl;
    private final String        writeDriver;
    private final String        dataFormat;
    private final Properties    properties;
    private final int           batchSize;
    private final SaveMode      writeMode;

    public LaunchpadDestination(
            @JsonProperty( NAME ) String name,
            @JsonProperty( WRITE_URL ) String writeUrl,
            @JsonProperty( WRITE_DRIVER ) String writeDriver,
            @JsonProperty( DATA_FORMAT ) Optional<String> dataFormat,
            @JsonProperty( WRITE_MODE ) Optional<String> writeMode,
            @JsonProperty( USER ) Optional<String> username,
            @JsonProperty( PASSWORD ) Optional<String> password,
            @JsonProperty( PROPERTIES ) Optional<Properties> properties,
            @JsonProperty( BATCH_SIZE ) Optional<Integer> batchSize ) {
        Preconditions.checkState( StringUtils.isNotBlank( name ), "Name must be specified for a desintation." );
        this.name = name;
        this.writeUrl = writeUrl;
        this.writeDriver = writeDriver;
        this.batchSize = batchSize.orElse( DEFAULT_DATA_CHUNK_SIZE );
        // if dataFormat not set, use writeDriver value
        this.dataFormat = dataFormat.orElse( writeDriver );
        this.properties = properties.orElse( new Properties() );
        this.writeMode = SaveMode.valueOf( writeMode.orElse( DEFAULT_WRITE_MODE.name()));

        this.properties.put(JDBC_URL, writeUrl);
        this.properties.put(MAXIMUM_POOL_SIZE, "1");
        this.properties.put(CONNECTION_TIMEOUT, "120000"); //2-minute connection timeout

        this.username = username.orElse( "" );
        this.password = password.orElse( "" );

        if ( !NON_JDBC_DRIVERS.contains( writeDriver ) && StringUtils.isBlank( this.username )){
            logger.warn( "connecting to " + name + " with blank username!");
        }
        if ( !NON_JDBC_DRIVERS.contains( writeDriver ) && StringUtils.isBlank( this.password )){
            logger.warn( "connecting to " + name + " with blank password!");
        }

        this.properties.setProperty( "user", this.username );
        this.properties.setProperty( USER, this.username );
        this.properties.setProperty( PASSWORD, this.password );
    }

    public DataLake asDataLake() {
        String lakeDataFormat = "";
        String lakeDriver = "";
        switch ( writeDriver ){
            case S3_DRIVER:
                lakeDriver = S3_DRIVER;
                lakeDataFormat = dataFormat;
                break;
            case CSV_FORMAT:
            case ORC_FORMAT:
                lakeDriver = FILESYSTEM_DRIVER;
                lakeDataFormat = writeDriver;
                break;
            case FILESYSTEM_DRIVER:
                lakeDriver = FILESYSTEM_DRIVER;
                lakeDataFormat = dataFormat;
                break;
            case LEGACY_CSV_FORMAT:
                lakeDriver = FILESYSTEM_DRIVER;
                lakeDataFormat = CSV_FORMAT;
                break;
            default: // JDBC
                lakeDriver = writeDriver;
                lakeDataFormat = writeDriver;
                break;
        }
        return new DataLake(
                name,
                writeUrl,
                lakeDriver,
                lakeDataFormat,
                username,
                password,
                false,
                DEFAULT_DATA_CHUNK_SIZE,
                batchSize,
                writeMode,
                false,
                properties);
    }

    @JsonProperty( WRITE_DRIVER )
    public String getWriteDriver() {
        return writeDriver;
    }

    @JsonProperty( DATA_FORMAT )
    public String getDataFormat() {
        return dataFormat;
    }

    @JsonProperty( WRITE_URL )
    public String getWriteUrl() {
        return writeUrl;
    }

    @JsonProperty( PROPERTIES )
    public Properties getProperties() {
        return properties;
    }

    @JsonProperty( BATCH_SIZE )
    public int getBatchSize() {
        return batchSize;
    }

    @JsonProperty( NAME )
    public String getName() {
        return name;
    }

    @JsonProperty( WRITE_MODE )
    public SaveMode getWriteMode() {
        return writeMode;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof LaunchpadDestination ) ) { return false; }
        LaunchpadDestination that = (LaunchpadDestination) o;
        return batchSize == that.batchSize &&
                Objects.equals( name, that.name ) &&
                Objects.equals( writeUrl, that.writeUrl ) &&
                Objects.equals( writeDriver, that.writeDriver ) &&
                Objects.equals( dataFormat, that.dataFormat) &&
                Objects.equals( properties, that.properties );
    }

    @Override public int hashCode() {
        return Objects.hash( name, writeUrl, writeDriver, dataFormat, properties, batchSize );
    }

    @JsonIgnore
    public HikariDataSource getHikariDatasource() {
        final Properties pClone = (Properties) properties.clone();
        pClone.setProperty( "username",pClone.getProperty( "user" )  );
        pClone.remove( "user" );
        HikariConfig hc = new HikariConfig( pClone );
        logger.info( "JDBC URL = {}", hc.getJdbcUrl() );
        return new HikariDataSource( hc );
    }

    @Override public String toString() {
        return "LaunchpadDestination{" +
                "writeUrl='" + writeUrl + '\'' +
                ", writeDriver='" + writeDriver + '\'' +
                ", writeMode='" + writeMode + '\'' +
                ", properties=" + properties +
                ", batchSize=" + batchSize +
                '}';
    }

}

