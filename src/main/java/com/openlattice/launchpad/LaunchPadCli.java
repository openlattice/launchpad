package com.openlattice.launchpad;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LaunchPadCli {
    public static String HELP    = "help";
    public static String FILE    = "file";

    private static Options           options = new Options();
    private static CommandLineParser clp     = new DefaultParser();
    private static HelpFormatter     hf      = new HelpFormatter();

    static {
        options.addOption( HELP, "Print help message." );
        options.addOption( FILE,
                true,
                "File in which the final model will be saved. Also used as prefix for intermediate saves of the model." );

    }

    private LaunchPadCli() {
    }

    public static CommandLine parseCommandLine( String[] args ) throws ParseException {
        return clp.parse( options, args );
    }

    public static void printHelp() {
        hf.printHelp( "launchpad", options );
    }
}
