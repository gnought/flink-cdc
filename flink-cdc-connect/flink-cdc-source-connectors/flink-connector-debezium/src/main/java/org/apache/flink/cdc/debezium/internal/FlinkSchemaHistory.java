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

import io.debezium.config.Configuration;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import static org.apache.flink.cdc.debezium.utils.SchemaHistoryUtil.registerHistory;
import static org.apache.flink.cdc.debezium.utils.SchemaHistoryUtil.removeHistory;
import static org.apache.flink.cdc.debezium.utils.SchemaHistoryUtil.retrieveHistory;

/**
 * Inspired from {@link io.debezium.relational.history.MemorySchemaHistory} but we will store the
 * HistoryRecords in Flink's state for persistence.
 *
 * <p>Note: This is not a clean solution because we depend on a global variable and all the history
 * records will be stored in state (grow infinitely). We may need to come up with a
 * FileSystemDatabaseHistory in the future to store history in HDFS.
 */
public class FlinkSchemaHistory extends AbstractSchemaHistory {

    private ConcurrentLinkedQueue<SchemaRecord> schemaRecords;
    private String instanceName;

    /** Gets the registered HistoryRecords under the given instance name. */
    private ConcurrentLinkedQueue<SchemaRecord> getRegisteredHistoryRecord(String instanceName) {
        Collection<SchemaRecord> historyRecords = retrieveHistory(instanceName);
        return new ConcurrentLinkedQueue<>(historyRecords);
    }

    @Override
    public void configure(
            Configuration config,
            HistoryRecordComparator comparator,
            SchemaHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        this.instanceName = config.getString(NAME);
        this.schemaRecords = getRegisteredHistoryRecord(instanceName);

        // register the schema changes into state
        // every change should be visible to the source function
        registerHistory(instanceName, schemaRecords);
    }

    @Override
    public void stop() {
        super.stop();
        removeHistory(instanceName);
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
        this.schemaRecords.add(new SchemaRecord(record));
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        this.schemaRecords.stream().map(SchemaRecord::getHistoryRecord).forEach(records);
    }

    @Override
    public boolean exists() {
        return !schemaRecords.isEmpty();
    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public String toString() {
        return "Flink Database History";
    }

    /** Determine whether the {@link FlinkSchemaHistory} is compatible with the specified state. */
    public static boolean isCompatible(Collection<SchemaRecord> records) {
        for (SchemaRecord record : records) {
            // check the source/position/ddl is not null
            if (!record.isHistoryRecord()) {
                return false;
            } else {
                break;
            }
        }
        return true;
    }
}