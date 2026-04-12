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
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class XlakeReadWriteE2ETest {

    private static SparkSession spark;

    private static final StructType SCHEMA = new StructType(new StructField[]{
            new StructField("key", DataTypes.StringType, false, Metadata.empty()),
            new StructField("value", DataTypes.StringType, true, Metadata.empty())
    });

    @TempDir
    Path tempDir;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("xlake-read-write-e2e")
                .config("spark.ui.enabled", "false")
                .config("spark.plugins", "io.github.ximin.xlake.backend.spark.XlakeSparkPlugin")
                .config("spark.xlake.routing.shardCount", "1")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @Test
    void writeAndReadBack() {
        String path = tempDir.resolve("writeAndReadBack").toString();
        String table = "testdb.write_read";

        writeData(path, table, List.of(
                RowFactory.create("k1", "v1"),
                RowFactory.create("k2", "v2"),
                RowFactory.create("k3", "v3"),
                RowFactory.create("k4", "v4"),
                RowFactory.create("k5", "v5")
        ));

        List<Row> got = readData(path, table).collectAsList();
        assertThat(got).hasSize(5);
        assertThat(got).extracting(r -> r.getString(0) + ":" + r.getString(1))
                .containsExactlyInAnyOrder("k1:v1", "k2:v2", "k3:v3", "k4:v4", "k5:v5");
    }

    @Test
    void emptyTableReturnsNoRows() {
        String path = tempDir.resolve("emptyTable").toString();
        String table = "testdb.empty_table";

        List<Row> got = readData(path, table).collectAsList();
        assertThat(got).isEmpty();
    }

    @Test
    void multipleBatchesAccumulate() {
        String path = tempDir.resolve("multiBatch").toString();
        String table = "testdb.multi_batch";

        for (int batch = 0; batch < 3; batch++) {
            List<Row> rows = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                int idx = batch * 3 + i;
                rows.add(RowFactory.create("key_" + idx, "val_" + idx));
            }
            writeData(path, table, rows);
        }

        List<Row> got = readData(path, table).collectAsList();
        assertThat(got).hasSize(9);
    }

    @Test
    void pointQueryByKey() {
        String path = tempDir.resolve("pointQuery").toString();
        String table = "testdb.point_query";

        writeData(path, table, List.of(
                RowFactory.create("k1", "v1"),
                RowFactory.create("k2", "v2"),
                RowFactory.create("k3", "v3"),
                RowFactory.create("k4", "v4"),
                RowFactory.create("k5", "v5")
        ));

        List<Row> got = readData(path, table).filter("key = 'k3'").collectAsList();
        assertThat(got).hasSize(1);
        assertThat(got.get(0).getString(0)).isEqualTo("k3");
        assertThat(got.get(0).getString(1)).isEqualTo("v3");
    }

    @Test
    void rangeQueryByKey() {
        String path = tempDir.resolve("rangeQuery").toString();
        String table = "testdb.range_query";

        writeData(path, table, List.of(
                RowFactory.create("k1", "v1"),
                RowFactory.create("k2", "v2"),
                RowFactory.create("k3", "v3"),
                RowFactory.create("k4", "v4"),
                RowFactory.create("k5", "v5")
        ));

        List<Row> got = readData(path, table).filter("key >= 'k2' AND key < 'k4'").collectAsList();
        assertThat(got).extracting(r -> r.getString(0))
                .containsExactlyInAnyOrder("k2", "k3");
    }

    @Test
    void fullTableScan() {
        String path = tempDir.resolve("fullScan").toString();
        String table = "testdb.full_scan";

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            rows.add(RowFactory.create("key_" + String.format("%02d", i), "val_" + i));
        }
        writeData(path, table, rows);

        List<Row> got = readData(path, table).collectAsList();
        assertThat(got).hasSize(10);
    }

    @Test
    void nullValueHandling() {
        String path = tempDir.resolve("nullValue").toString();
        String table = "testdb.null_value";

        writeData(path, table, List.of(
                RowFactory.create("k1", "v1"),
                RowFactory.create("k_null", null)
        ));

        List<Row> got = readData(path, table).collectAsList();
        assertThat(got).hasSize(2);

        Row nullRow = got.stream()
                .filter(r -> "k_null".equals(r.getString(0)))
                .findFirst()
                .orElseThrow();
        assertThat(nullRow.isNullAt(1)).isTrue();
    }

    @Test
    void duplicateKeyOverwrite() {
        String path = tempDir.resolve("dupKey").toString();
        String table = "testdb.dup_key";

        writeData(path, table, List.of(RowFactory.create("k1", "v1")));
        writeData(path, table, List.of(RowFactory.create("k1", "v2")));

        List<Row> got = readData(path, table).collectAsList();
        Row k1 = got.stream()
                .filter(r -> "k1".equals(r.getString(0)))
                .findFirst()
                .orElseThrow();
        assertThat(k1.getString(1)).isEqualTo("v2");
    }

    private void writeData(String path, String table, List<Row> rows) {
        spark.createDataFrame(rows, SCHEMA)
                .write()
                .format("xlake")
                .option("path", path)
                .option("table", table)
                .mode("append")
                .save();
    }

    private Dataset<Row> readData(String path, String table) {
        return spark.read()
                .format("xlake")
                .option("path", path)
                .option("table", table)
                .load();
    }
}
