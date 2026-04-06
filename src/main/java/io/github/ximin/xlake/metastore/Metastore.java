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
package io.github.ximin.xlake.metastore;

import io.github.ximin.xlake.meta.*;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface Metastore {

    void createTable(PbTableMetadata metadata) throws IOException;

    void dropTable(String catalog, String database, String table) throws IOException;

    Optional<PbTableMetadata> getTable(String catalog, String database, String table) throws IOException;

    void alterTable(String catalog, String database, String table, PbSchema newSchema) throws IOException;

    List<String> listTables(String catalog, String database) throws IOException;

    Optional<PbSnapshot> getSnapshot(String catalog, String database, String table, long snapshotId) throws IOException;

    List<PbSnapshot> getSnapshots(String catalog, String database, String table) throws IOException;

    long createSnapshot(String catalog, String database, String table, String operation, String summary) throws IOException;

    long beginCommit(List<PbTableOperation> operations) throws IOException;

    boolean commit(long commitId) throws IOException;

    boolean abortCommit(long commitId) throws IOException;

    void putFile(PbFileMetadata fileMeta) throws IOException;

    Optional<PbFileMetadata> getFile(String tableName, String filePath) throws IOException;

    List<PbFileMetadata> listFiles(String tableName) throws IOException;

    List<PbFileMetadata> listFiles(String tableName, int level) throws IOException;

    void removeFile(String tableName, String filePath) throws IOException;

    void putUpdateEntry(PbUpdateEntry entry) throws IOException;

    Optional<PbUpdateEntry> getUpdateEntry(String entryId) throws IOException;

    List<PbUpdateEntry> getUpdateEntries(String tableName) throws IOException;

    void deleteUpdateEntry(String entryId) throws IOException;

    void close() throws IOException;
}
