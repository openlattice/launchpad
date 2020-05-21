package com.openlattice.launchpad

import com.fasterxml.jackson.module.kotlin.readValue
import com.openlattice.launchpad.configuration.DataLake
import com.openlattice.launchpad.configuration.IntegrationConfiguration
import com.openlattice.launchpad.serialization.JacksonSerializationConfiguration
import org.apache.commons.cli.*
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */

class LaunchpadCli {
    companion object {
        const val HELP    = "help"
        const val FILE    = "file"
        const val USERNAMES = "usernames"
        const val PASSWORDS = "passwords"

        private val options = Options()

        init {
            options.addOption(HELP, "Print help message." );
            options.addOption( Option.builder(FILE)
                    .hasArg()
                    .required()
                    .desc("File in which the final model will be saved. Also used as prefix for intermediate saves of the model." )
                    .build());
            options.addOption( Option.builder(USERNAMES)
                    .hasArg()
                    .desc("Usernames for connection to the client database. Formatted as a json map {<dataLakeName>:<username>,...}")
                    .build());
            options.addOption( Option.builder(PASSWORDS)
                    .hasArg()
                    .desc("Passwords for connection to the client database. Formatted as a json map {<dataLakeName>:<password>,...}")
                    .build());
        }

        @JvmStatic
        fun parseCommandLine(args: Array<String> ) : CommandLine {
            try {
                return DefaultParser().parse(options, args )
            } catch ( ex: MissingOptionException ) {
                println("Integration file must be specified!")
                System.exit(-1)
            }
            return CommandLine.Builder().build()
        }

        @JvmStatic
        fun printHelp(): Unit {
            HelpFormatter().printHelp( "launchpad", options);
        }

        @JvmStatic
        fun readUsernamesPasswordsAndUpdateConfiguration(config: IntegrationConfiguration, cl: CommandLine ): IntegrationConfiguration {
            val mapper = JacksonSerializationConfiguration.jsonMapper
            var lakeToUsername: Map<String, String> = mapOf()
            if (cl.hasOption(USERNAMES) && cl.getOptionValue(USERNAMES).isNotBlank() ) {
                lakeToUsername = mapper.readValue(cl.getOptionValue(USERNAMES))
            }

            var lakeToPassword: Map<String, String> = mapOf()
            if (cl.hasOption(PASSWORDS) && cl.getOptionValue(PASSWORDS).isNotBlank() ) {
                lakeToPassword = mapper.readValue(cl.getOptionValue(PASSWORDS))
            }
            val newLakes = config.datalakes.get().map {
                val username = lakeToUsername.get( it.name )
                val password = lakeToPassword.get( it.name )
                DataLake.withUsernameAndPassword( it, username, password )
            }
            return IntegrationConfiguration(
                    config.name,
                    config.description,
                    config.awsConfig,
                    config.datasources,
                    config.destinations,
                    Optional.of(newLakes),
                    config.integrations
            )
        }
    }
}