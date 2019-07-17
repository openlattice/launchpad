package com.openlattice.launchpad.postgres

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

import com.google.common.base.Stopwatch
import com.google.common.collect.ImmutableList
import org.slf4j.LoggerFactory

import java.io.Closeable
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.util.concurrent.TimeUnit

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StatementHolder @JvmOverloads constructor(
        val connection: Connection,
        val statement: Statement,
        val resultSet: ResultSet,
        private val otherStatements: List<Statement> = ImmutableList.of(),
        private val otherResultSets: List<ResultSet> = ImmutableList.of(),
        private val longRunningQueryLimit: Long = LONG_RUNNING_QUERY_LIMIT_MILLIS) : Closeable {

    private val sw = Stopwatch.createStarted()
    var isOpen = true
        private set

    constructor(
            connection: Connection,
            statement: Statement,
            resultSet: ResultSet,
            longRunningQueryLimit: Long) : this(connection, statement, resultSet, ImmutableList.of<Statement>(), ImmutableList.of<ResultSet>(), longRunningQueryLimit) {
    }

    @Synchronized
    override fun close() {
        if (isOpen) {
            otherResultSets.forEach { safeTryClose(it) }
            otherStatements.forEach { safeTryClose(it) }

            val elapsed = sw.elapsed(TimeUnit.MILLISECONDS)
            if (elapsed > this.longRunningQueryLimit) {
                logger.warn("The following statement was involved in a long lived connection that took {} ms: {}",
                        elapsed,
                        statement.toString())
            }

            sw.stop()
            safeTryClose(resultSet)
            safeTryClose(statement)
            safeTryClose(connection)
            isOpen = false
        }
    }

    private fun safeTryClose(obj: AutoCloseable) {
        try {
            obj.close()
        } catch (e: Exception) {
            logger.error("Unable to close obj {}", obj, e)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(StatementHolder::class.java)
        private val LONG_RUNNING_QUERY_LIMIT_MILLIS: Long = 15000
    }
}
