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

import static com.google.common.base.Preconditions.checkState;
import static com.openlattice.launchpad.LaunchPad.CSV_DRIVER;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.tools.picocli.CommandLine.MissingParameterException;

/**
 * Represents a name for data integrations.
 */
public class LaunchpadDatasource {
    private static final String NAME       = "name";
    private static final String URL        = "url";
    private static final String DRIVER     = "driver";
    private static final String USER       = "username";
    private static final String PASSWORD   = "password";
    private static final String FETCH_SIZE = "fetchSize";
    private static final String HEADER     = "header";

    private final String     name;
    private final String     url;
    private final String     driver;
    private final String     password;
    private final String     user;
    private final int        fetchSize;
    private final boolean    header;
    private final Properties properties;

    public LaunchpadDatasource(
            @JsonProperty( NAME ) String name,
            @JsonProperty( URL ) String url,
            @JsonProperty( DRIVER ) String driver,
            @JsonProperty( USER ) Optional<String> user,
            @JsonProperty( PASSWORD ) Optional<String> password,
            @JsonProperty( FETCH_SIZE ) Optional<Integer> fetchSize,
            @JsonProperty( HEADER ) Optional<Boolean> header ) {
        checkState( header.map( hasHeader -> hasHeader && CSV_DRIVER.equals( driver ) ).get(),
                "header can only be set for csv" );
        this.name = name;
        this.url = url;
        this.driver = driver;
        if ( !StringUtils.equals( CSV_DRIVER, driver ) ) {
            this.user = user.orElseThrow( () -> new MissingParameterException(
                    "A username must be specified for database connections." ) );
        } else {
            //User can be blank for CSV.
            this.user = "";
        }
        //Depending on server configuration a password may not be required to establish a connection.
        this.password = password.orElse( "" );
        this.fetchSize = fetchSize.orElse( 20000 );

        properties = new Properties();
        properties.setProperty( "user", this.user );
        properties.setProperty( "password", this.password );
        properties.setProperty( "driver", this.driver );
        this.header = header.orElse( false );
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

    @JsonIgnore
    public Properties getProperties() {
        return properties;
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
                Objects.equals( user, that.user ) &&
                Objects.equals( properties, that.properties );
    }

    @Override public int hashCode() {
        return Objects.hash( name, url, driver, password, user, fetchSize, properties );
    }

    @Override public String toString() {
        return "LaunchpadDatasource{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", driver='" + driver + '\'' +
                ", password='" + password + '\'' +
                ", user='" + user + '\'' +
                ", fetchSize=" + fetchSize +
                ", properties=" + properties +
                '}';
    }
}
