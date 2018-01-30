package com.openlattice.coupler;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Integration {
    private static final String NAME     = "name";
    private static final String URL      = "url";
    private static final String DRIVER   = "driver";
    private static final String USER     = "user";
    private static final String PASSWORD = "password";

    private static final String SQL          = "sql";
    private static final String WRITE_URL    = "writeUrl";
    private static final String WRITE_DRIVER = "writeDriver";
    private static final String HIKARI       = "hikari";
    private static final String FETCH_SIZE   = "fetchSize";
    private static       Logger logger       = LoggerFactory.getLogger( Integration.class );
    private final String     name;
    private final String     sql;
    private final Properties hikari;
    private final String     url;
    private final String     password;
    private final String     writeUrl;
    private final String     user;
    private final String     driver;
    private final String     writeDriver;
    private final int        fetchSize;

    @JsonCreator
    public Integration(
            @JsonProperty( NAME ) String name,
            @JsonProperty( URL ) String url,
            @JsonProperty( DRIVER ) String driver,
            @JsonProperty( USER ) String user,
            @JsonProperty( PASSWORD ) String password,
            @JsonProperty( WRITE_URL ) String writeUrl,
            @JsonProperty( WRITE_DRIVER ) String writeDriver,
            @JsonProperty( FETCH_SIZE ) int fetchSize,
            @JsonProperty( SQL ) String sql,
            @JsonProperty( HIKARI ) Properties hikari ) {
        this.name = name;
        this.url = url;
        this.sql = sql;
        this.driver = driver;
        this.user = user;
        this.password = password;
        this.fetchSize = fetchSize;
        this.writeUrl = writeUrl;
        this.writeDriver = writeDriver;
        this.hikari = hikari;
    }

    @JsonProperty( FETCH_SIZE )
    public int getFetchSize() {
        return fetchSize;
    }

    @JsonProperty( NAME )
    public String getName() {
        return name;
    }

    @JsonProperty( SQL )
    public String getSql() {
        return sql;
    }

    @JsonIgnore
    public HikariDataSource getHikariDatasource() {
        HikariConfig hc = new HikariConfig( hikari );
        logger.info( "JDBC URL = {}", hc.getJdbcUrl() );
        return new HikariDataSource( hc );
    }

    @JsonProperty( URL )
    public String getUrl() {
        return url;
    }

    @JsonProperty( PASSWORD )
    public String getPassword() {
        return password;
    }

    @JsonProperty( DRIVER )
    public String getDriver() {
        return driver;
    }

    @JsonProperty( USER )
    public String getUser() {
        return user;
    }

    @JsonProperty( WRITE_DRIVER )
    public String getWriteDriver() {
        return writeDriver;
    }

    @JsonProperty( WRITE_URL )
    public String getWriteUrl() {
        return writeUrl;
    }

    @JsonProperty( HIKARI )
    public Properties getProperties() {
        return hikari;
    }
}
