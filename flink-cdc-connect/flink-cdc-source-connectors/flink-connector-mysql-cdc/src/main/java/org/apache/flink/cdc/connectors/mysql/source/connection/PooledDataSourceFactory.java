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

package org.apache.flink.cdc.connectors.mysql.source.connection;

import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.util.FlinkRuntimeException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;

import java.sql.DriverManager;

/** A connection pool factory to create pooled DataSource {@link HikariDataSource}. */
public class PooledDataSourceFactory {

    public static final String CONNECTION_POOL_PREFIX = "connection-pool-";
    public static final String SERVER_TIMEZONE_KEY = "serverTimezone";
    public static final int MINIMUM_POOL_SIZE = 1;

    private PooledDataSourceFactory() {}

    public static HikariDataSource createPooledDataSource(MySqlSourceConfig sourceConfig) {
        final HikariConfig config = new HikariConfig();

        String hostName = sourceConfig.getHostname();
        int port = sourceConfig.getPort();
        config.setPoolName(CONNECTION_POOL_PREFIX + hostName + ":" + port);

        MySqlConnection conn = DebeziumUtils.createMySqlConnection(sourceConfig);
        config.setJdbcUrl(conn.connectionString());

        config.setUsername(sourceConfig.getUsername());
        config.setPassword(sourceConfig.getPassword());
        config.setMinimumIdle(MINIMUM_POOL_SIZE);
        config.setMaximumPoolSize(sourceConfig.getConnectionPoolSize());
        config.setConnectionTimeout(sourceConfig.getConnectTimeout().toMillis());
        config.addDataSourceProperty(SERVER_TIMEZONE_KEY, sourceConfig.getServerTimeZone());
        DriverManager.getDrivers();
        config.setDriverClassName(conn.config().getString(MySqlConnectorConfig.JDBC_DRIVER));

        // optional optimization configurations for pooled DataSource
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        try {
            return new HikariDataSource(config);
        } catch (HikariPool.PoolInitializationException e) {
            throw new FlinkRuntimeException(
                    "Initialize jdbc connection pool failed, this may caused by"
                            + " wrong jdbc configurations or unstable network."
                            + " Please check your jdbc configurations and network.",
                    e);
        }
    }
}
