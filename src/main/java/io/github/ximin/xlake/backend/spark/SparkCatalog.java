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
package io.github.ximin.xlake.backend.spark;

import io.github.ximin.xlake.common.catalog.CatalogArgs;
import io.github.ximin.xlake.common.catalog.XlakeCatalog;
import io.github.ximin.xlake.table.TableId;
import io.github.ximin.xlake.table.XlakeTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;

import java.util.Set;

@Slf4j
public class SparkCatalog implements StagingTableCatalog {
    private XlakeCatalog delegatedCatalog;
    private SparkSession spark;

    @Override
    public Set<TableCatalogCapability> capabilities() {
        return StagingTableCatalog.super.capabilities();
    }

    @Override
    public Identifier[] listTables(String[] strings) throws NoSuchNamespaceException {
        return new Identifier[0];
    }

    @Override
    public Table loadTable(Identifier identifier) throws NoSuchTableException {
        XlakeTable table = delegatedCatalog.loadXlakeTable(toXlakeTableId(identifier));
        return null;
    }

    @Override
    public Table alterTable(Identifier identifier, TableChange... tableChanges) throws NoSuchTableException {
        return null;
    }

    @Override
    public boolean dropTable(Identifier identifier) {
        return false;
    }

    @Override
    public void renameTable(Identifier identifier, Identifier identifier1) throws NoSuchTableException, TableAlreadyExistsException {

    }

    @Override
    public void initialize(String s, CaseInsensitiveStringMap caseInsensitiveStringMap) {
        CatalogArgs catalogArgs = CatalogArgs.fromMap(caseInsensitiveStringMap.asCaseSensitiveMap());
        catalogArgs.getCatalogType();
        Option<SparkSession> sparkOpt = SparkSession.getActiveSession();
        if (sparkOpt.isEmpty()) {
            log.warn("Spark session is null!");
        } else {
            this.spark = sparkOpt.get();
        }

    }

    @Override
    public String name() {
        return "";
    }

    private TableId toXlakeTableId(Identifier identifier) {
        if (identifier.namespace().length > 0) {
            return new TableId(identifier.namespace()[0], identifier.name());
        }
        throw new RuntimeException("no namespace found in identifier");
    }
}
