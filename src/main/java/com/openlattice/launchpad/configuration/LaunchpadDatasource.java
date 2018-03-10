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
import java.util.Optional;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LaunchpadDatasource {
    private static final String NAME       = "name";
    private static final String URL        = "url";
    private static final String DRIVER     = "driver";
    private static final String SQL        = "sql";
    private static final String USER       = "user";
    private static final String PASSWORD   = "password";
    private static final String FETCH_SIZE = "fetchSize";

    private final String name;
    private final String url;
    private final String driver;
    private final String sql;
    private final String password;
    private final String user;
    private final int    fetchSize;

    public LaunchpadDatasource(
            @JsonProperty( NAME ) Optional<String> name,
            @JsonProperty( URL ) String url,
            @JsonProperty( DRIVER ) String driver,
            @JsonProperty( SQL ) String sql,
            @JsonProperty( USER ) String user,
            @JsonProperty( PASSWORD ) String password,
            @JsonProperty( FETCH_SIZE ) Optional<Integer> fetchSize ) {
        this.name = name.orElse( "Unnamed Datasource" );
        this.url = url;
        this.sql = sql;
        this.driver = driver;
        this.user = user;
        this.password = password;
        this.fetchSize = fetchSize.orElse( 20000 );
    }

    @JsonProperty( FETCH_SIZE )
    public int getFetchSize() {
        return fetchSize;
    }

    @JsonProperty( URL )
    public String getUrl() {
        return url;
    }

    @JsonProperty( SQL )
    public String getSql() {
        return sql;
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

}
