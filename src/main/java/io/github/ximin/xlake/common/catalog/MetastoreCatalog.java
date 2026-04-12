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
package io.github.ximin.xlake.common.catalog;

import io.github.ximin.xlake.common.exception.CatalogException;
import io.github.ximin.xlake.common.exception.CatalogIOException;
import io.github.ximin.xlake.common.exception.InvalidMetadataException;
import io.github.ximin.xlake.common.exception.TableNotFoundException;
import io.github.ximin.xlake.meta.PbSnapshot;
import io.github.ximin.xlake.meta.PbTableMetadata;
import io.github.ximin.xlake.metastore.Metastore;
import io.github.ximin.xlake.table.*;
import io.github.ximin.xlake.table.impl.XlakeTableImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

// TODO 实现表实例缓存设计
@Slf4j
public final class MetastoreCatalog implements XlakeCatalog, AutoCloseable {


    private final MetastoreCatalogConfig config;


    private volatile boolean closed = false;


    public MetastoreCatalog(MetastoreCatalogConfig config) {
        this.config = Objects.requireNonNull(config, "config cannot be null");
        log.info("MetastoreCatalog initialized (Phase 0 - Serializable Architecture): catalogName={}",
                config.catalogName());
    }


    @Override
    public XlakeTable loadXlakeTable(TableId identifier) {
        ensureOpen();

        log.debug("Loading XlakeTable (Serializable): {}.{}",
                identifier.database(), identifier.tableName());

        validateTableIdentifier(identifier);

        String catalogName = config.catalogName();
        String databaseName = identifier.database();
        String tableName = identifier.tableName();

        PbTableMetadata pbMeta = loadTableMetadataFromMetastore(catalogName, databaseName, tableName);
        TableMeta tableMeta = convertToTableMeta(pbMeta);

        DynamicTableInfo dynamicInfo = loadDynamicInfoFromMetastore(catalogName, databaseName, tableName);

        return createSerializableTableInstance(identifier, tableMeta, dynamicInfo);
    }


    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            log.info("Closing catalog (lightweight - no storage engine): {}", config.catalogName());
        }
    }


    private void validateTableIdentifier(TableId identifier) {
        if (identifier == null) {
            throw new IllegalArgumentException("Table identifier cannot be null");
        }
        if (identifier.database() == null || identifier.database().isBlank()) {
            throw new IllegalArgumentException("Database name cannot be null or blank");
        }
        if (identifier.tableName() == null || identifier.tableName().isBlank()) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
    }


    private PbTableMetadata loadTableMetadataFromMetastore(String catalogName,
                                                           String databaseName,
                                                           String tableName) {
        try {
            Metastore metastore = config.metastore();
            Optional<PbTableMetadata> metaOpt = metastore.getTable(catalogName, databaseName, tableName);

            if (metaOpt.isEmpty()) {
                log.warn("Table not found in metastore: {}.{}.{}",
                        catalogName, databaseName, tableName);
                throw new TableNotFoundException(
                        String.format("Table '%s.%s' not found in catalog '%s'",
                                databaseName, tableName, catalogName),
                        String.format("%s.%s.%s", catalogName, databaseName, tableName)
                );
            }

            PbTableMetadata pbMeta = metaOpt.get();
            log.debug("Successfully loaded metadata for table: {}.{}.{}",
                    catalogName, databaseName, tableName);
            return pbMeta;

        } catch (TableNotFoundException e) {
            // 业务异常直接传播，不包装
            throw e;
        } catch (java.io.IOException e) {
            // I/O异常转换为领域异常
            log.error("I/O error while accessing metastore for table: {}.{}.{}",
                    catalogName, databaseName, tableName, e);
            throw new CatalogIOException(
                    String.format("Metastore access failed for '%s.%s': %s",
                            databaseName, tableName, e.getMessage()),
                    "Metastore",
                    e
            );
        } catch (Exception e) {
            // 其他异常（如反序列化错误）视为元数据无效
            log.error("Unexpected error while loading metadata from metastore: {}.{}.{}",
                    catalogName, databaseName, tableName, e);
            throw new InvalidMetadataException(
                    String.format("Failed to parse metadata for '%s.%s': %s",
                            databaseName, tableName, e.getMessage()),
                    String.format("%s.%s.%s", catalogName, databaseName, tableName),
                    e
            );
        }
    }


    private TableMeta convertToTableMeta(PbTableMetadata pbMeta) {
        try {
            TableMeta tableMeta = ProtoTableMetaConverter.fromPb(pbMeta);
            log.debug("Converted PbTableMetadata to TableMeta: {} (tableId={})",
                    tableMeta.fullName(), tableMeta.tableId());
            return tableMeta;
        } catch (Exception e) {
            log.error("Failed to convert PbTableMetadata to TableMeta", e);
            throw new InvalidMetadataException(
                    "Failed to parse table metadata: " + e.getMessage(),
                    e
            );
        }
    }


    private DynamicTableInfo loadDynamicInfoFromMetastore(String catalogName,
                                                          String databaseName,
                                                          String tableName) {
        try {
            Metastore metastore = config.metastore();
            List<PbSnapshot> pbSnapshots = metastore.getSnapshots(catalogName, databaseName, tableName);

            if (pbSnapshots == null || pbSnapshots.isEmpty()) {
                log.debug("No snapshots found in metastore for table: {}.{}.{}",
                        catalogName, databaseName, tableName);
                return DynamicTableInfo.builder().build();
            }

            // 转换PbSnapshot列表为业务对象
            List<Snapshot> snapshots = pbSnapshots.stream()
                    .filter(Objects::nonNull)
                    .map(ProtoTableMetaConverter::fromPbSnapshot)
                    .toList();

            DynamicTableInfo dynamicInfo = DynamicTableInfo.builder()
                    .withSnapshots(snapshots)
                    .build();

            log.debug("Loaded dynamic info for table: {}.{}.{} (snapshotCount={})",
                    catalogName, databaseName, tableName, snapshots.size());

            return dynamicInfo;

        } catch (Exception e) {
            log.warn("Failed to load dynamic info for table: {}.{}.{}, returning empty state",
                    catalogName, databaseName, tableName, e);
            // 返回空的DynamicTableInfo而不是抛异常，允许表以初始状态使用
            return DynamicTableInfo.builder().build();
        }
    }


    private XlakeTableImpl createSerializableTableInstance(TableId identifier,
                                                           TableMeta tableMeta,
                                                           DynamicTableInfo dynamicInfo) {
        try {
            String tableIdentifier = buildTableIdentifier(identifier);

            XlakeTableImpl table = new XlakeTableImpl(
                    tableIdentifier,
                    tableMeta,
                    dynamicInfo  // 动态信息在构造时传入，避免Table持有Metastore依赖
            );

            log.info("Successfully created serializable XlakeTable instance: {} (tableId={}, type={})",
                    tableIdentifier,
                    tableMeta.tableId(),
                    tableMeta.tableType());

            return table;

        } catch (Exception e) {
            log.error("Failed to create XlakeTable instance for: {}.{}",
                    identifier.database(), identifier.tableName(), e);
            throw new CatalogException(
                    String.format("Failed to create table instance for '%s.%s': %s",
                            identifier.database(), identifier.tableName(), e.getMessage()),
                    e
            );
        }
    }


    private String buildTableIdentifier(TableId identifier) {
        return String.format("%s.%s.%s",
                config.catalogName(),
                identifier.database(),
                identifier.tableName());
    }


    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException(
                    "Catalog is closed: " + config.catalogName());
        }
    }
}
