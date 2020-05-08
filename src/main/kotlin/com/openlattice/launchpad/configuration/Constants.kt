package com.openlattice.launchpad.configuration

import com.google.common.collect.Sets
import org.apache.spark.sql.SaveMode

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
object Constants  {
    const val URL = "url"
    const val NAME = "name"
    const val USER = "user"
    const val USERNAME = "username"
    const val DRIVER = "driver"
    const val PASSWORD = "password"
    const val PROPERTIES = "properties"
    const val WRITE_MODE = "writeMode"
    const val BATCH_SIZE = "batchSize"
    const val FETCH_SIZE = "fetchSize"
    const val DATA_FORMAT = "dataFormat"
    const val DESCRIPTION = "description"
    const val DESTINATION = "destination"
    const val SOURCE = "source"
    const val HEADER = "header"
    const val JDBC_URL = "jdbcUrl"
    const val MAXIMUM_POOL_SIZE = "maximumPoolSize"
    const val CONNECTION_TIMEOUT = "connectionTimeout"

    const val LEGACY_CSV_FORMAT = "com.openlattice.launchpad.csv"
    const val CSV_FORMAT = "csv"
    const val ORC_FORMAT = "orc"
    const val FILESYSTEM_DRIVER = "filesystem"
    const val S3_DRIVER = "s3"
    const val UNKNOWN = "unknown"

    const val DEFAULT_DATA_CHUNK_SIZE = 20_000

    @JvmField
    val DEFAULT_WRITE_MODE = SaveMode.Overwrite

    @JvmField
    val NON_JDBC_DRIVERS: Set<String>   = Sets.newHashSet(S3_DRIVER, FILESYSTEM_DRIVER)
}
