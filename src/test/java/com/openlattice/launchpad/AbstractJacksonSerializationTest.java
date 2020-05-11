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

package com.openlattice.launchpad;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.launchpad.serialization.JacksonSerializationConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public abstract class AbstractJacksonSerializationTest<T> {
    protected static final ObjectMapper mapper = JacksonSerializationConfiguration.mapper;
    protected static final ObjectMapper smile  = JacksonSerializationConfiguration.smileMapper;
    protected static final ObjectMapper yaml   = JacksonSerializationConfiguration.yamlMapper;
    protected final        Logger       logger = LoggerFactory.getLogger( getClass() );

    @Test
    public void testSerdes() throws IOException {
        T data = getSampleData();
        SerializationResult<T> result = serialize( data );
        Assert.assertEquals( data, result.deserializeJsonString( getClazz() ) );
        Assert.assertEquals( data, result.deserializeYamlString( getClazz() ) );
        Assert.assertEquals( data, result.deserializeJsonBytes( getClazz() ) );
        Assert.assertEquals( data, result.deserializeSmileBytes( getClazz() ) );
    }

    protected SerializationResult<T> serialize( T data ) throws IOException {
        return new SerializationResult<T>( mapper.writeValueAsString( data ),
                yaml.writeValueAsString( data ),
                mapper.writeValueAsBytes( data ),
                smile.writeValueAsBytes( data ) );
    }

    protected abstract T getSampleData();

    protected abstract Class<T> getClazz();

    protected static class SerializationResult<T> {
        private final String jsonString;
        private final byte[] jsonBytes;
        private final byte[] smileBytes;
        private final String yamlString;

        public SerializationResult( String jsonString, String yamlString, byte[] jsonBytes, byte[] smileBytes ) {
            this.jsonString = jsonString;
            this.yamlString = yamlString;
            this.jsonBytes = Arrays.copyOf( jsonBytes, jsonBytes.length );
            this.smileBytes = Arrays.copyOf( smileBytes, smileBytes.length );
        }

        protected T deserializeJsonString( Class<T> clazz ) throws IOException {
            return mapper.readValue( jsonString, clazz );
        }

        protected T deserializeJsonBytes( Class<T> clazz ) throws IOException {
            return mapper.readValue( jsonBytes, clazz );
        }

        protected T deserializeSmileBytes( Class<T> clazz ) throws IOException {
            return smile.readValue( smileBytes, clazz );
        }

        protected T deserializeYamlString( Class<T> clazz ) throws IOException {
            return yaml.readValue( yamlString, clazz );
        }

        public String getJsonString() {
            return jsonString;
        }
    }
}
    
