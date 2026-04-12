/*-
 * #%L
 * xlake-demo
 * %%
 * Copyright (C) 2026 ximin1024
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package io.github.ximin.xlake.table.impl;

import io.github.ximin.xlake.common.exception.CatalogException;
import io.github.ximin.xlake.storage.table.TableStore;
import io.github.ximin.xlake.table.DynamicTableInfo;
import io.github.ximin.xlake.table.XlakeTable;
import io.github.ximin.xlake.table.Snapshot;
import io.github.ximin.xlake.table.TableMeta;
import io.github.ximin.xlake.table.op.Op;
import io.github.ximin.xlake.table.op.OpResult;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;


@Slf4j
public class XlakeTableImpl implements XlakeTable, Serializable {

    private static final long serialVersionUID = 1L;

    
    private final String tableIdentifier;

    
    private final TableMeta meta;

    
    private volatile DynamicTableInfo dynamicInfo;

    
    private transient ReentrantReadWriteLock lock;

    
    private transient AtomicBoolean closed;

    
    public XlakeTableImpl(String tableIdentifier, TableMeta meta,
                          DynamicTableInfo dynamicInfo) {
        this.tableIdentifier = Objects.requireNonNull(tableIdentifier, "tableIdentifier cannot be null");
        this.meta = Objects.requireNonNull(meta, "meta cannot be null");
        this.dynamicInfo = dynamicInfo != null ? dynamicInfo : DynamicTableInfo.builder().build();

        // 初始化transient字段
        this.lock = new ReentrantReadWriteLock();
        this.closed = new AtomicBoolean(false);

        log.debug("XlakeTableImpl initialized (Serializable): {} (tableId={}, type={})",
            tableIdentifier,
            meta.tableId(),
            meta.tableType());
    }

    @Override
    public TableMeta meta() {
        return meta;
    }

    @Override
    public Snapshot currentSnapshot() {
        lock.readLock().lock();
        try {
            return dynamicInfo != null ? dynamicInfo.currentSnapshot() : null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public DynamicTableInfo dynamicInfo() {
        lock.readLock().lock();
        try {
            return dynamicInfo;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        lock.readLock().lock();
        try {
            if (dynamicInfo == null) {
                return null;
            }
            return dynamicInfo.snapshots().stream()
                    .filter(s -> s.snapshotId() == snapshotId)
                    .findFirst()
                    .orElse(null);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<Snapshot> snapshots() {
        lock.readLock().lock();
        try {
            return dynamicInfo != null ? dynamicInfo.snapshots() : List.of();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public <R extends OpResult> R op(Op<R> op) {
        ensureOpen();
        Objects.requireNonNull(op, "op cannot be null");
        return op.exec();
    }

    
    @Override
    @Deprecated
    public void refresh() throws CatalogException {
        ensureOpen();
        throw new CatalogException(
            "Direct call to refresh() is deprecated in serializable Table mode. " +
            "Use Refresh op via table.op(Refresh.builder().withTable(this).build()) instead.\n\n" +
            "Example:\n" +
            "  table.op(Refresh.builder()\n" +
            "      .withTable(this)\n" +
            "      .clientSupplier(() -> yourMetastoreClient)\n" +
            "      .build());\n\n" +
            "Reason: XlakeTableImpl no longer holds Metastore reference for serialization support. " +
            "All dependencies must be injected via TableOp mechanism."
        );
    }
    
    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            log.info("Closing XlakeTableImpl (lightweight - no storage resources): {}", tableIdentifier);
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    
    public void updateDynamicInfo(DynamicTableInfo newInfo) {
        Objects.requireNonNull(newInfo, "newInfo cannot be null");
        lock.writeLock().lock();
        try {
            this.dynamicInfo = newInfo;
            log.debug("Dynamic info updated for table: {}, snapshotCount={}",
                tableIdentifier, newInfo.snapshots().size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    
    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Table is closed: " + tableIdentifier);
        }
    }

    
    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        // 1. 执行默认的反序列化（恢复非transient字段）
        in.defaultReadObject();

        // 2. 重建transient字段
        this.lock = new ReentrantReadWriteLock();
        this.closed = new AtomicBoolean(false);

        log.debug("Reconstructed transient fields after deserialization: {}", tableIdentifier);
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    @Override
    public String toString() {
        return "XlakeTableImpl{" +
                "tableIdentifier='" + tableIdentifier + '\'' +
                ", tableId=" + meta.tableId() +
                ", tableName='" + meta.tableName() + '\'' +
                ", closed=" + closed.get() +
                ", serializable=true" +  // 标记为可序列化
                '}';
    }
}
