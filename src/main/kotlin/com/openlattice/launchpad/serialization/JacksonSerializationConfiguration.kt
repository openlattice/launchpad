package com.openlattice.launchpad.serialization

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.afterburner.AfterburnerModule
import com.fasterxml.jackson.module.kotlin.KotlinModule

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class JacksonSerializationConfiguration {

    companion object {
        @JvmField
        public val mapper = createJsonMapper()

        @JvmField
        public val smileMapper = createSmileMapper()

        @JvmField
        public val yamlMapper = createYamlMapper()

        protected fun createYamlMapper(): ObjectMapper {
            return registerModules(ObjectMapper(YAMLFactory()))
        }

        protected fun createSmileMapper(): ObjectMapper {
            return registerModules(ObjectMapper(SmileFactory()))
        }

        protected fun createJsonMapper(): ObjectMapper {
            return registerModules(ObjectMapper())
        }

        protected fun registerModules(mapper: ObjectMapper): ObjectMapper {
            mapper.registerModule(Jdk8Module())
            mapper.registerModule(GuavaModule())
            mapper.registerModule(JodaModule())
            mapper.registerModule(AfterburnerModule())
            mapper.registerModule(KotlinModule())
            return mapper
        }
    }
}