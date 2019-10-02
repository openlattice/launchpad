package com.openlattice.launchpad.configuration


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IntegrationTables {
    companion object {
        /**
         * SQL query for creating integration table.
         */
        const val CREATE_INTEGRATION_ACTIVITY_SQL = "CREATE TABLE IF NOT EXISTS integration_activity " +
                "(integration_name text, host_name text, table_name text, start timestamptz DEFAULT 'now()', finish timestamptz DEFAULT 'infinity', result text, PRIMARY KEY (integration_name, host_name, table_name, start))"

        /**
         * Preparable sql query that logs when an integration begins with the following bind parameters:
         *
         * 1. integration_name
         * 2. host_name
         * 3. table_name
         * 4. start
         */
        const val LOG_INTEGRATION_STARTED  = "INSERT INTO integration_activity (integration_name, host_name, table_name, start) " +
                "VALUES (?,?,?,?)"

        /**
         * Preparable sql query for logging a successfully completed integration with the following bind parameters:
         *
         * 1. integration_name
         * 2. host_name
         * 3. table_name
         * 4. start
         */
        const val LOG_SUCCESSFUL_INTEGRATION = "UPDATE integration_activity " +
                "SET finish = now(), result = 'SUCCESS' " +
                "WHERE integration_name = ? " +
                    "AND host_name = ? " +
                    "AND table_name = ? " +
                    "AND start = ? "

        /**
         * Preparable sql query for logging a failed integration with the following bind parameters:
         *
         * 1. integration_name
         * 2. host_name
         * 3. table_name
         * 4. start
         */
        const val LOG_FAILED_INTEGRATION = "UPDATE integration_activity SET finish = now(), result = 'FAILED' WHERE integration_name = ? AND host_name = ? AND table_name = ? AND start = ? "
    }
}