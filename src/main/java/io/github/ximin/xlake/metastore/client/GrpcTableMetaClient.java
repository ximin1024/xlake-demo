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
package io.github.ximin.xlake.metastore.client;

import io.github.ximin.xlake.meta.PbOperationResult;
import io.github.ximin.xlake.meta.PbTableMetadata;
import io.github.ximin.xlake.meta.PbTableOperation;

import java.util.List;

public class GrpcTableMetaClient implements TableMetaClient {

    public GrpcTableMetaClient() {
    }

    @Override
    public void createTable(PbTableMetadata metadata) {
        throw new UnsupportedOperationException("GrpcTableMetaClient.createTable not implemented");
    }

    @Override
    public void dropTable(String tableName) {
        throw new UnsupportedOperationException("GrpcTableMetaClient.dropTable not implemented");
    }

    @Override
    public PbOperationResult alterTable(PbTableOperation operation) {
        throw new UnsupportedOperationException("GrpcTableMetaClient.alterTable not implemented");
    }

    @Override
    public PbOperationResult commitOperations(String tableName, long commitId, List<PbTableOperation> operations) {
        throw new UnsupportedOperationException("GrpcTableMetaClient.commitOperations not implemented");
    }

    @Override
    public void refreshTable(String tableName) {
        throw new UnsupportedOperationException("GrpcTableMetaClient.refreshTable not implemented");
    }

    @Override
    public void abortTransaction(String tableName, long transactionId) {
        throw new UnsupportedOperationException("GrpcTableMetaClient.abortTransaction not implemented");
    }

    @Override
    public PbTableMetadata getTableMetadata(String tableName) {
        throw new UnsupportedOperationException("GrpcTableMetaClient.getTableMetadata not implemented");
    }
}
