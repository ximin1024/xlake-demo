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

import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.StructType;

public class SparkScanBuilder implements ScanBuilder,
        SupportsPushDownV2Filters,
        SupportsPushDownRequiredColumns {
    private static final Predicate[] EMPTY_PREDICATES = new Predicate[0];
    private Predicate[] pushedPredicates = EMPTY_PREDICATES;

    @Override
    public Scan build() {
        return null;
    }

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
        return new Predicate[0];
    }

    @Override
    public Predicate[] pushedPredicates() {
        return this.pushedPredicates;
    }

    @Override
    public void pruneColumns(StructType structType) {

    }
}
