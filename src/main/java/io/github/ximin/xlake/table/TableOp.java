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
package io.github.ximin.xlake.table;

import io.github.ximin.xlake.metastore.MetastoreClient;

import java.util.function.Supplier;

public record TableOp(String tableName, Supplier<MetastoreClient> clientSupplier) {

//    public TableOp(String tableName) {
//        this(tableName, MetastoreClient::getInstance);
//    }
//
//    public Commit.CommitBuilder commitBuilder() {
//        return Commit.builder()
//                .tableName(tableName)
//                .clientSupplier(clientSupplier);
//    }
}
