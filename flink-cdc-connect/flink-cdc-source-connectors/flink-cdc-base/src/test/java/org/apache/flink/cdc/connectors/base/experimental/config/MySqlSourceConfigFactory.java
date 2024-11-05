/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.base.experimental.config;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory;
import org.apache.flink.cdc.connectors.base.experimental.EmbeddedFlinkSchemaHistory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.engine.DebeziumEngine;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.SchemaHistory;

import java.util.Properties;
import java.util.UUID;

import static io.debezium.config.CommonConnectorConfig.TOPIC_PREFIX;
import static org.apache.flink.cdc.connectors.base.utils.EnvironmentUtils.checkSupportCheckpointsAfterTasksFinished;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A factory to initialize {@link MySqlSourceConfig}. */
public class MySqlSourceConfigFactory extends JdbcSourceConfigFactory {

    private ServerIdRange serverIdRange;

    /**
     * A numeric ID or a numeric ID range of this database client, The numeric ID syntax is like
     * '5400', the numeric ID range syntax is like '5400-5408', The numeric ID range syntax is
     * required when 'scan.incremental.snapshot.enabled' enabled. Every ID must be unique across all
     * currently-running database processes in the MySQL cluster. This connector joins the MySQL
     * cluster as another server (with this unique ID) so it can read the binlog.
     */
    public MySqlSourceConfigFactory serverId(String serverId) {
        this.serverIdRange = ServerIdRange.from(serverId);
        return this;
    }

    /** Creates a new {@link MySqlSourceConfig} for the given subtask {@code subtaskId}. */
    public MySqlSourceConfig create(int subtaskId) {
        checkSupportCheckpointsAfterTasksFinished(closeIdleReaders);
        Properties props = new Properties();
        // hard code server name, because we don't need to distinguish it, docs:
        // Logical name that identifies and provides a namespace for the particular
        // MySQL database server/cluster being monitored. The logical name should be
        // unique across all other connectors, since it is used as a prefix for all
        // Kafka topic names emanating from this connector.
        // Only alphanumeric characters and underscores should be used.
        props.setProperty(TOPIC_PREFIX.name(), "mysql_binlog_source");
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty(
                CommonConnectorConfig.DRIVER_CONFIG_PREFIX + "fetchSize",
                String.valueOf(fetchSize));
        props.setProperty(
                CommonConnectorConfig.DRIVER_CONFIG_PREFIX + "responseBuffering", "adaptive");
        props.setProperty("database.connectionTimeZone", serverTimeZone);
        // schema history
        props.setProperty(
                HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY.name(),
                EmbeddedFlinkSchemaHistory.class.getCanonicalName());
        props.setProperty(SchemaHistory.NAME.name(), UUID.randomUUID() + "_" + subtaskId);
        props.setProperty(
                SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS.name(), String.valueOf(true));
        props.setProperty("schema.history.refer.ddl", String.valueOf(true));
        props.setProperty(
                MySqlConnectorConfig.CONNECTION_TIMEOUT_MS.name(),
                String.valueOf(connectTimeout.toMillis()));
        // the underlying debezium reader should always capture the schema changes and forward them.
        // Note: the includeSchemaChanges parameter is used to control emitting the schema record,
        // only DataStream API program need to emit the schema record, the Table API need not
        props.setProperty(
                RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES.name(),
                String.valueOf(true));
        // disable the offset flush totally
        props.setProperty(
                DebeziumEngine.OFFSET_FLUSH_INTERVAL_MS_PROP, String.valueOf(Long.MAX_VALUE));
        // disable tombstones
        props.setProperty(CommonConnectorConfig.TOMBSTONES_ON_DELETE.name(), String.valueOf(false));
        // debezium use "long" mode to handle unsigned bigint by default,
        // but it'll cause lose of precise when the value is larger than 2^63,
        // so use "precise" mode to avoid it.
        props.put(
                MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE.name(),
                MySqlConnectorConfig.BigIntUnsignedHandlingMode.PRECISE.getValue());

        if (serverIdRange != null) {
            props.setProperty("database.server.id.range", String.valueOf(serverIdRange));
            int serverId = serverIdRange.getServerId(subtaskId);
            props.setProperty(MySqlConnectorConfig.SERVER_ID.name(), String.valueOf(serverId));
        }
        if (databaseList != null) {
            props.setProperty(
                    RelationalDatabaseConnectorConfig.DATABASE_INCLUDE_LIST.name(),
                    String.join(",", databaseList));
        }
        if (tableList != null) {
            props.setProperty(
                    RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST.name(),
                    String.join(",", tableList));
        }
        if (serverTimeZone != null) {
            props.setProperty("database.connectionTimeZone", serverTimeZone);
        }

        // override the user-defined debezium properties
        if (dbzProperties != null) {
            dbzProperties.forEach(props::put);
        }

        Configuration dbzConfiguration = Configuration.from(props);
        String driverClassName = dbzConfiguration.getString(MySqlConnectorConfig.JDBC_DRIVER);
        return new MySqlSourceConfig(
                startupOptions,
                databaseList,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                props,
                dbzConfiguration,
                driverClassName,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                scanNewlyAddedTableEnabled);
    }
}
