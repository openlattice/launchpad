package com.openlattice.launchpad

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */

class LaunchPadCli {
    companion object {
        const val HELP    = "help"
        const val FILE    = "file"

        private val options = Options()
        private val clp     = DefaultParser()
        private val hf      = HelpFormatter()

        init {
            options.addOption(HELP, "Print help message." );
            options.addOption(FILE,
                    true,
                    "File in which the final model will be saved. Also used as prefix for intermediate saves of the model." );
        }

        @JvmStatic
        fun parseCommandLine(args: Array<String> ) : CommandLine {
            return clp.parse(options, args )
        }

        @JvmStatic
        fun printHelp(): Unit {
            hf.printHelp( "launchpad", options);
        }
    }
}