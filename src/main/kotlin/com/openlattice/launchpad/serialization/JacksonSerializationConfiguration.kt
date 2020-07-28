package com.openlattice.launchpad.serialization

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.afterburner.AfterburnerModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.openlattice.launchpad.configuration.Constants

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class JacksonSerializationConfiguration {

    companion object {
        @JvmField
        public val jsonMapper = createJsonMapper()

        @JvmField
        public val smileMapper = createSmileMapper()

        @JvmField
        public val yamlMapper = createYamlMapper()

        @JvmField
        public val credentialFilteredJsonMapper = createCredentialFilteredJsonMapper()

        protected fun createYamlMapper(): ObjectMapper {
            return registerModules(ObjectMapper(YAMLFactory()))
        }

        protected fun createSmileMapper(): ObjectMapper {
            return registerModules(ObjectMapper(SmileFactory()))
        }

        protected fun createJsonMapper(): ObjectMapper {
            return registerModules(ObjectMapper())
        }

        public fun createCredentialFilteredJsonMapper(): ObjectMapper {
            return registerModules(ObjectMapper(), setOf(Constants.USERNAME, Constants.PASSWORD, Constants.ACCESS_KEY_ID, Constants.SECRET_ACCESS_KEY, Constants.PROPERTIES))
        }

        protected fun registerModules(mapper: ObjectMapper, filteredProperties: Set<String> = setOf()): ObjectMapper {
            val filterProvider = SimpleFilterProvider().addFilter( Constants.CREDENTIALS_FILTER, SimpleBeanPropertyFilter.SerializeExceptFilter(filteredProperties))
            mapper.registerModule(Jdk8Module())
            mapper.registerModule(GuavaModule())
            mapper.registerModule(JodaModule())
            mapper.registerModule(AfterburnerModule())
            mapper.registerModule(KotlinModule())
            mapper.setFilterProvider(filterProvider)
            return mapper
        }
    }
}