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
package io.github.ximin.xlake.backend;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class XlakeDataSourceV2E2ETest {
    @Test
    void writeThenReadBack(@TempDir Path tempDir) {
        SparkSession spark = SparkSession.builder()
                .master("local[1]")
                .appName("xlake-sparkv2-e2e")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        try {
            List<Row> rows = List.of(
                    RowFactory.create("k1", "v1"),
                    RowFactory.create("k2", "v2")
            );
            StructType schema = new StructType(new StructField[]{
                    new StructField("key", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("value", DataTypes.StringType, true, Metadata.empty())
            });

            spark.createDataFrame(rows, schema)
                    .write()
                    .format("xlake")
                    .option("path", tempDir.toString())
                    .option("table", "testdb.testtable")
                    .mode("append")
                    .save();

            Dataset<Row> out = spark.read()
                    .format("xlake")
                    .option("path", tempDir.toString())
                    .option("table", "testdb.testtable")
                    .load();

            List<Row> got = out.collectAsList();
            assertThat(got).hasSize(2);
            assertThat(got).extracting(r -> r.getString(0) + ":" + r.getString(1))
                    .containsExactlyInAnyOrder("k1:v1", "k2:v2");
        } finally {
            spark.stop();
        }
    }
}

