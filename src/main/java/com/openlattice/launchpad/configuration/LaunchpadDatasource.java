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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.CSV_FORMAT;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.LEGACY_CSV_FORMAT;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.DEFAULT_DATA_CHUNK_SIZE;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.DEFAULT_WRITE_MODE;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.S3_DRIVER;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.FILESYSTEM_DRIVER;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.ORC_FORMAT;
import static com.openlattice.launchpad.configuration.IntegrationConfigurationKt.UNKNOWN;

/**
 * Represents a name for data integrations.
 */
@Deprecated
public class LaunchpadDatasource {
    private static final String NAME       = "name";
    private static final String URL        = "url";
    private static final String DRIVER     = "driver";
    private static final String USER       = "username";
    private static final String PASSWORD   = "password";
    private static final String FETCH_SIZE = "fetchSize";
    private static final String HEADER     = "header";

    private static final Logger logger = LoggerFactory.getLogger( LaunchpadDestination.class );

    private final String     name;
    private final String     url;
    private final String     driver;
    private final String     password;
    private final String     user;
    private final int        fetchSize;
    private final boolean    header;

    public LaunchpadDatasource(
            @JsonProperty( NAME ) String name,
            @JsonProperty( URL ) String url,
            @JsonProperty( DRIVER ) String driver,
            @JsonProperty( USER ) Optional<String> user,
            @JsonProperty( PASSWORD ) Optional<String> password,
            @JsonProperty( FETCH_SIZE ) Optional<Integer> fetchSize,
            @JsonProperty( HEADER ) Optional<Boolean> header ) {
        checkState( header.map( hasHeader ->
                        !hasHeader || ( hasHeader && (CSV_FORMAT.equals( driver ) || LEGACY_CSV_FORMAT.equals( driver )))
                ).orElse( true ), "header can only be set for csv" );
        this.name = name;
        this.url = url;
        this.driver = driver;
        if ( !StringUtils.equals( CSV_FORMAT, driver ) && !StringUtils.equals( LEGACY_CSV_FORMAT, driver )) {
            this.user = user.orElseThrow( () ->
                    new IllegalStateException("A username must be specified for database connections." ) );
        } else {
            //User can be blank for CSV.
            this.user = "";
        }
        //Depending on server configuration a password may not be required to establish a connection.
        this.password = password.orElse( "" );
        this.fetchSize = fetchSize.orElse( DEFAULT_DATA_CHUNK_SIZE );
        this.header = header.orElse( false );
    }

    public DataLake asDataLake() {
        String lakeDataFormat = "";
        String lakeDriver = "";
        switch ( driver ){
            case S3_DRIVER:
                lakeDriver = S3_DRIVER;
                lakeDataFormat = UNKNOWN;
                break;
            case CSV_FORMAT:
            case ORC_FORMAT:
                lakeDriver = FILESYSTEM_DRIVER;
                lakeDataFormat = CSV_FORMAT;
                break;
            case FILESYSTEM_DRIVER:
                lakeDriver = FILESYSTEM_DRIVER;
                lakeDataFormat = UNKNOWN;
                break;
            case LEGACY_CSV_FORMAT:
                lakeDriver = FILESYSTEM_DRIVER;
                lakeDataFormat = CSV_FORMAT;
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
                user,
                password,
                header,
                fetchSize,
                DEFAULT_DATA_CHUNK_SIZE,
                DEFAULT_WRITE_MODE,
                false,
                new Properties());
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

    @JsonProperty( USER )
    public String getUser() {
        return user;
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
                Objects.equals( user, that.user );
    }

    @Override public int hashCode() {
        return Objects.hash( name, url, driver, password, user, fetchSize);
    }

    @Override public String toString() {
        return "LaunchpadDatasource{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", driver='" + driver + '\'' +
                ", password='" + password + '\'' +
                ", user='" + user + '\'' +
                ", fetchSize=" + fetchSize +
                '}';
    }
}
