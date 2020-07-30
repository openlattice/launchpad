package com.openlattice.launchpad.configuration


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IntegrationTables {
    companion object {

        const val INTEGRATION_STATUS_TABLE_NAME = "integration_activity"

        /**
         * SQL for appending v2 columns to existing table
         */
        const val UPGRADE_V2= """
            ALTER TABLE $INTEGRATION_STATUS_TABLE_NAME
            ADD COLUMN IF NOT EXISTS configuration jsonb not null default '{}'::jsonb, 
            ADD COLUMN IF NOT EXISTS stacktrace text
        """

        val upgrades = arrayOf(
                UPGRADE_V2
        )

        /**
         * SQL query for creating integration table.
         */
        const val CREATE_INTEGRATION_ACTIVITY_SQL = """ 
            CREATE TABLE IF NOT EXISTS $INTEGRATION_STATUS_TABLE_NAME
                ( integration_name text, host_name text, table_name text, start timestamptz DEFAULT now(), finish timestamptz DEFAULT 'infinity', result text, configuration json, stacktrace text, 
            PRIMARY KEY (integration_name, host_name, table_name, start))
        """

        /**
         * Preparable sql query that logs when an integration begins with the following bind parameters:
         *
         * 1. integration_name
         * 2. host_name
         * 3. table_name
         * 4. start
         * 5. configuration as json
         */
        const val LOG_INTEGRATION_STARTED  = """
            INSERT INTO $INTEGRATION_STATUS_TABLE_NAME
                (integration_name, host_name, table_name, start, configuration)
            VALUES (?,?,?,?,?::jsonb)
        """

        /**
         * Preparable sql query for logging a successfully completed integration with the following bind parameters:
         *
         * 1. integration_name
         * 2. host_name
         * 3. table_name
         * 4. start
         */
        const val LOG_SUCCESSFUL_INTEGRATION = """
            UPDATE $INTEGRATION_STATUS_TABLE_NAME
            SET finish = now(), result = 'SUCCESS' 
            WHERE integration_name = ? 
                AND host_name = ? 
                AND table_name = ? 
                AND start = ? 
        """

        /**
         * Preparable sql query for logging a failed integration with the following bind parameters:
         *
         * 1. stacktrace
         * 2. integration_name
         * 3. host_name
         * 4. table_name
         * 5. start
         */
        const val LOG_FAILED_INTEGRATION = """
            UPDATE $INTEGRATION_STATUS_TABLE_NAME
            SET finish = now(), result = 'FAILED', stacktrace = ?
            WHERE integration_name = ? 
                AND host_name = ? 
                AND table_name = ? 
                AND start = ? 
        """
    }
}