package com.openlattice.launchpad

import org.apache.commons.cli.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */

class LaunchpadCli {
    companion object {
        const val HELP = "help"
        const val FILE = "file"

        private val options = Options()
        private val clp = DefaultParser()
        private val hf = HelpFormatter()

        init {
            options.addOption(
                    Option.builder("h")
                            .longOpt(HELP)
                            .desc("Print help message.")
                            .build()
            )
            options.addOption(
                    Option.builder("f")
                            .longOpt(FILE)
                            .hasArg(true)
                            .desc("File in which the final model will be saved. Also used as prefix for intermediate saves of the model.")
                            .build()
            )
        }

        @JvmStatic
        fun parseCommandLine(args: Array<String>): CommandLine {
            return clp.parse(options, args)
        }

        @JvmStatic
        fun printHelp() {
            hf.printHelp("launchpad", options);
        }
    }
}