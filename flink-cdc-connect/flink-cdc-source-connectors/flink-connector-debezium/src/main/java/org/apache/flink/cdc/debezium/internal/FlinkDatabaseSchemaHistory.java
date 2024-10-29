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

package org.apache.flink.cdc.debezium.internal;

import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.DatabaseSchema;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.debezium.relational.history.TableChanges.TableChange;
import static org.apache.flink.cdc.debezium.utils.DatabaseHistoryUtil.registerHistory;
import static org.apache.flink.cdc.debezium.utils.DatabaseHistoryUtil.removeHistory;
import static org.apache.flink.cdc.debezium.utils.DatabaseHistoryUtil.retrieveHistory;

/**
 * The {@link FlinkDatabaseSchemaHistory} only stores the latest schema of the monitored tables.
 * When recovering from the checkpoint, it should apply all the tables to the {@link
 * DatabaseSchema}, which doesn't need to replay the history anymore.
 *
 * <p>Considering the data structure maintained in the {@link FlinkDatabaseSchemaHistory} is much
 * different from the {@link FlinkDatabaseHistory}, it's not compatible with the {@link
 * FlinkDatabaseHistory}. Because it only maintains the latest schema of the table rather than all
 * history DDLs, it's useful to prevent OOM when meet massive history DDLs.
 */
public class FlinkDatabaseSchemaHistory implements SchemaHistory {

    public static final String DATABASE_HISTORY_INSTANCE_NAME = "database.history.instance.name";

    private final FlinkJsonTableChangeSerializer tableChangesSerializer =
            new FlinkJsonTableChangeSerializer();

    private ConcurrentMap<TableId, SchemaRecord> latestTables;
    private String instanceName;
    private SchemaHistoryListener listener;
    private boolean storeOnlyMonitoredTablesDdl;
    private boolean skipUnparseableDDL;
    private boolean useCatalogBeforeSchema;

    @Override
    public void configure(
            Configuration config,
            HistoryRecordComparator comparator,
            SchemaHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        this.instanceName = config.getString(DATABASE_HISTORY_INSTANCE_NAME);
        this.listener = listener;
        this.storeOnlyMonitoredTablesDdl = config.getBoolean(STORE_ONLY_CAPTURED_TABLES_DDL);
        this.skipUnparseableDDL = config.getBoolean(SKIP_UNPARSEABLE_DDL_STATEMENTS);
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;

        // recover
        this.latestTables = new ConcurrentHashMap<>();
        for (SchemaRecord schemaRecord : retrieveHistory(instanceName)) {
            // validate here
            TableChange tableChange =
                    FlinkJsonTableChangeSerializer.fromDocument(
                            schemaRecord.toDocument(), useCatalogBeforeSchema);
            latestTables.put(tableChange.getId(), schemaRecord);
        }
        // register
        registerHistory(instanceName, latestTables.values());
    }

    @Override
    public void start() {
        listener.started();
    }

    @Override
    public void record(
            Map<String, ?> source, Map<String, ?> position, String databaseName, String ddl)
            throws SchemaHistoryException {
        throw new UnsupportedOperationException(
                String.format(
                        "The %s cannot work with 'debezium.internal.implementation' = 'legacy',"
                                + "please use %s",
                        FlinkDatabaseSchemaHistory.class.getCanonicalName(),
                        FlinkDatabaseHistory.class.getCanonicalName()));
    }

    @Override
    public void record(
            Map<String, ?> source,
            Map<String, ?> position,
            String databaseName,
            String schemaName,
            String ddl,
            TableChanges changes,
            Instant timestamp)
            throws SchemaHistoryException {
        for (TableChanges.TableChange change : changes) {
            switch (change.getType()) {
                case CREATE:
                case ALTER:
                    latestTables.put(
                            change.getId(),
                            new SchemaRecord(tableChangesSerializer.toDocument(change)));
                    break;
                case DROP:
                    latestTables.remove(change.getId());
                    break;
                default:
                    // impossible
                    throw new RuntimeException(
                            String.format("Unknown change type: %s.", change.getType()));
            }
        }
        listener.onChangeApplied(
                new HistoryRecord(
                        source, position, databaseName, schemaName, ddl, changes, timestamp));
    }

    @Override
    public void recover(
            Map<String, ?> source, Map<String, ?> position, Tables schema, DdlParser ddlParser) {
        listener.recoveryStarted();
        for (SchemaRecord record : latestTables.values()) {
            TableChange tableChange =
                    FlinkJsonTableChangeSerializer.fromDocument(
                            record.getTableChangeDoc(), useCatalogBeforeSchema);
            schema.overwriteTable(tableChange.getTable());
        }
        listener.recoveryStopped();
    }

    @Override
    public void recover(
            Map<Map<String, ?>, Map<String, ?>> offsets, Tables schema, DdlParser ddlParser) {
        listener.recoveryStarted();
        for (SchemaRecord record : latestTables.values()) {
            TableChange tableChange =
                    FlinkJsonTableChangeSerializer.fromDocument(
                            record.getTableChangeDoc(), useCatalogBeforeSchema);
            schema.overwriteTable(tableChange.getTable());
        }
        listener.recoveryStopped();
    }

    @Override
    public void stop() {
        if (instanceName != null) {
            removeHistory(instanceName);
        }
        listener.stopped();
    }

    @Override
    public boolean exists() {
        return latestTables != null && !latestTables.isEmpty();
    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public void initializeStorage() {
        // do nothing
    }

    @Override
    public boolean storeOnlyCapturedTables() {
        return storeOnlyMonitoredTablesDdl;
    }

    @Override
    public boolean skipUnparseableDdlStatements() {
        return skipUnparseableDDL;
    }

    /**
     * Determine whether the {@link FlinkDatabaseSchemaHistory} is compatible with the specified
     * state.
     */
    public static boolean isCompatible(Collection<SchemaRecord> records) {
        for (SchemaRecord record : records) {
            if (!record.isTableChangeRecord()) {
                return false;
            } else {
                break;
            }
        }
        return true;
    }
}
