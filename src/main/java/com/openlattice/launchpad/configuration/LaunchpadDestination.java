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
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LaunchpadDestination {
    private static final String WRITE_URL    = "url";
    private static final String WRITE_DRIVER = "driver";
    private static final String USER       = "username";
    private static final String PASSWORD   = "password";

    private static final String PROPERTIES   = "properties";
    private static final String WRITE_TABLE  = "table";
    private static final String BATCH_SIZE ="batchSize";
    private static final int DEFAULT_BATCH_SIZE = 20000;
    private static       Logger logger       = LoggerFactory.getLogger( LaunchpadDestination.class );
    private final String            writeUrl;
    private final String            writeDriver;
    private final String            writeTable;
    private final Properties        properties;
    private final int batchSize;

    public LaunchpadDestination(
            @JsonProperty( WRITE_URL ) String writeUrl,
            @JsonProperty( WRITE_DRIVER ) String writeDriver,
            @JsonProperty( WRITE_TABLE ) String writeTable,
            @JsonProperty( USER ) Optional<String> username,
            @JsonProperty( PASSWORD ) Optional<String> password,
            @JsonProperty( PROPERTIES ) Optional<Properties> properties,
            @JsonProperty( BATCH_SIZE ) Optional<Integer> batchSize ) {
        this.writeUrl = writeUrl;
        this.writeDriver = writeDriver;
        this.writeTable = writeTable;
        this.batchSize = batchSize.orElse( DEFAULT_BATCH_SIZE );
        this.properties = properties.orElse( new Properties(  ) );
        username.ifPresent( u -> this.properties.setProperty("user", u) );
        password.ifPresent( p -> this.properties.setProperty(PASSWORD, p) );
    }

    @JsonProperty( WRITE_DRIVER )
    public String getWriteDriver() {
        return writeDriver;
    }

    @JsonProperty( WRITE_URL )
    public String getWriteUrl() {
        return writeUrl;
    }

    @JsonProperty( PROPERTIES )
    public Properties getProperties() {
        return properties;
    }

    @JsonProperty( WRITE_TABLE )
    public String getWriteTable() {
        return writeTable;
    }

    @JsonProperty( BATCH_SIZE )
    public int getBatchSize() {
        return batchSize;
    }

    @JsonIgnore
    public HikariDataSource getHikariDatasource() {
        HikariConfig hc = new HikariConfig( properties );
        logger.info( "JDBC URL = {}", hc.getJdbcUrl() );
        return new HikariDataSource( hc );
    }

    @Override public String toString() {
        return "LaunchpadDestination{" +
                "writeUrl='" + writeUrl + '\'' +
                ", writeDriver='" + writeDriver + '\'' +
                ", writeTable='" + writeTable + '\'' +
                ", properties=" + properties +
                ", batchSize=" + batchSize +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof LaunchpadDestination ) ) { return false; }
        LaunchpadDestination that = (LaunchpadDestination) o;
        return batchSize == that.batchSize &&
                Objects.equals( writeUrl, that.writeUrl ) &&
                Objects.equals( writeDriver, that.writeDriver ) &&
                Objects.equals( writeTable, that.writeTable ) &&
                Objects.equals( properties, that.properties );
    }

    @Override public int hashCode() {

        return Objects.hash( writeUrl, writeDriver, writeTable, properties, batchSize );
    }
}
