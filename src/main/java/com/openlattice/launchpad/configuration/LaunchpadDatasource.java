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

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static com.openlattice.launchpad.configuration.Constants.CONNECTION_TIMEOUT;
import static com.openlattice.launchpad.configuration.Constants.CSV_FORMAT;
import static com.openlattice.launchpad.configuration.Constants.DEFAULT_DATA_CHUNK_SIZE;
import static com.openlattice.launchpad.configuration.Constants.DEFAULT_WRITE_MODE;
import static com.openlattice.launchpad.configuration.Constants.DRIVER;
import static com.openlattice.launchpad.configuration.Constants.FETCH_SIZE;
import static com.openlattice.launchpad.configuration.Constants.FILESYSTEM_DRIVER;
import static com.openlattice.launchpad.configuration.Constants.HEADER;
import static com.openlattice.launchpad.configuration.Constants.LEGACY_CSV_FORMAT;
import static com.openlattice.launchpad.configuration.Constants.MAXIMUM_POOL_SIZE;
import static com.openlattice.launchpad.configuration.Constants.NAME;
import static com.openlattice.launchpad.configuration.Constants.NON_JDBC_DRIVERS;
import static com.openlattice.launchpad.configuration.Constants.ORC_FORMAT;
import static com.openlattice.launchpad.configuration.Constants.PASSWORD;
import static com.openlattice.launchpad.configuration.Constants.S3_DRIVER;
import static com.openlattice.launchpad.configuration.Constants.UNKNOWN;
import static com.openlattice.launchpad.configuration.Constants.URL;
import static com.openlattice.launchpad.configuration.Constants.USER;
import static com.openlattice.launchpad.configuration.Constants.USERNAME;

/**
 * Represents a name for data integrations.
 */
@Deprecated
@JsonFilter(Constants.CREDENTIALS_FILTER)
public class LaunchpadDatasource {
    private static final Logger logger = LoggerFactory.getLogger( LaunchpadDestination.class );

    private final String     name;
    private final String     url;
    private final String     driver;
    private final String     password;
    private final String     username;
    private final int        fetchSize;
    private final boolean    header;

    public LaunchpadDatasource(
            @JsonProperty( NAME ) String name,
            @JsonProperty( URL ) String url,
            @JsonProperty( DRIVER ) String driver,
            @JsonProperty( USERNAME ) Optional<String> username,
            @JsonProperty( PASSWORD ) Optional<String> password,
            @JsonProperty( FETCH_SIZE ) Optional<Integer> fetchSize,
            @JsonProperty( HEADER ) Optional<Boolean> header ) {
        this.name = name;
        this.url = url;
        this.driver = driver;
        this.header = header.orElse( false );

        //Depending on server configuration a password may not be required to establish a connection.
        this.password = password.orElse( "" );
        this.fetchSize = fetchSize.orElse( DEFAULT_DATA_CHUNK_SIZE );

        // JDBC datasource
        if ( !NON_JDBC_DRIVERS.contains( driver ) ){
            Preconditions.checkState( username.isPresent() && !StringUtils.isBlank( username.get() ),
                    "A username must be specified for database connections.");
            if ( StringUtils.isBlank( this.password )){
                logger.warn( "connecting to " + name + " with blank password!");
            }
        }

        //User can be blank for non-jdbc sources.
        this.username = username.orElse( "" );
    }

    public DataLake asDataLake() {

        final Properties properties = new Properties();
        properties.put(MAXIMUM_POOL_SIZE, "1");
        properties.put(CONNECTION_TIMEOUT, "120000"); //2-minute connection timeout
        properties.setProperty( USER, username );
        properties.setProperty( USERNAME, username );
        properties.setProperty( PASSWORD, password );

        String lakeDataFormat = "";
        String lakeDriver = "";
        switch ( driver ){
            case S3_DRIVER:
                lakeDriver = S3_DRIVER;
                lakeDataFormat = UNKNOWN;
                break;
            case CSV_FORMAT:
            case ORC_FORMAT:
            case LEGACY_CSV_FORMAT:
                lakeDriver = FILESYSTEM_DRIVER;
                lakeDataFormat = CSV_FORMAT;
                break;
            case FILESYSTEM_DRIVER:
                lakeDriver = FILESYSTEM_DRIVER;
                lakeDataFormat = UNKNOWN;
                break;
            default: // JDBC
                lakeDriver = driver;
                lakeDataFormat = driver;
                break;
        }
        return new DataLake(
                name,
                url,
                lakeDriver,
                lakeDataFormat,
                username,
                password,
                header,
                fetchSize,
                DEFAULT_DATA_CHUNK_SIZE,
                DEFAULT_WRITE_MODE,
                false,
                properties);
    }

    @JsonProperty( HEADER )
    public boolean isHeader() {
        return header;
    }

    @JsonProperty( NAME )
    public String getName() {
        return name;
    }

    @JsonProperty( FETCH_SIZE )
    public int getFetchSize() {
        return fetchSize;
    }

    @JsonProperty( URL )
    public String getUrl() {
        return url;
    }

    @JsonProperty( DRIVER )
    public String getDriver() {
        return driver;
    }

    @JsonProperty( USERNAME )
    public String getUser() {
        return username;
    }

    @JsonProperty( PASSWORD )
    public String getPassword() {
        return password;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof LaunchpadDatasource ) ) { return false; }
        LaunchpadDatasource that = (LaunchpadDatasource) o;
        return fetchSize == that.fetchSize &&
                Objects.equals( name, that.name ) &&
                Objects.equals( url, that.url ) &&
                Objects.equals( driver, that.driver ) &&
                Objects.equals( password, that.password ) &&
                Objects.equals( username, that.username );
    }

    @Override public int hashCode() {
        return Objects.hash( name, url, driver, password, username, fetchSize);
    }

    @Override public String toString() {
        return "LaunchpadDatasource{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", driver='" + driver + '\'' +
                ", password='" + password + '\'' +
                ", username='" + username + '\'' +
                ", fetchSize=" + fetchSize +
                '}';
    }
}
