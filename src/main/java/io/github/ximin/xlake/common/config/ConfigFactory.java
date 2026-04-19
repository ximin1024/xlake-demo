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
package io.github.ximin.xlake.common.config;

import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class ConfigFactory {

    public static XlakeConfig fromSparkConf(SparkConf sparkConf) {
        Map<String, Object> values = new HashMap<>();
        Tuple2<String, String>[] sparkPrefixEntries = sparkConf.getAllWithPrefix("spark.xlake.");
        for (Tuple2<String, String> entry : sparkPrefixEntries) {
            values.put(entry._1(), entry._2());
        }
        Tuple2<String, String>[] directPrefixEntries = sparkConf.getAllWithPrefix("xlake.");
        for (Tuple2<String, String> entry : directPrefixEntries) {
            values.put(entry._1(), entry._2());
        }
        return XlakeConfig.fromMap(values);
    }

    public static XlakeConfig fromMap(Map<String, String> map) {
        Map<String, Object> values = new HashMap<>();
        values.putAll(map);
        return XlakeConfig.fromMap(values);
    }

    public static XlakeConfig fromSystemProperties() {
        Map<String, Object> values = new HashMap<>();
        System.getProperties().stringPropertyNames().stream()
                .filter(k -> k.startsWith("xlake."))
                .forEach(k -> values.put(k, System.getProperty(k)));
        return XlakeConfig.fromMap(values);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static XlakeConfig createDefault() {
        return builder()
                .set(XlakeOptions.CLUSTER_ID, "default")
                .set(XlakeOptions.ZOOKEEPER_CONNECT_STRING, "127.0.0.1:2181")
                .set(XlakeOptions.ZOOKEEPER_NAMESPACE, "ximin")
                .set(XlakeOptions.METASTORE_TYPE, "memory")
                .set(XlakeOptions.SHARD_COUNT, 1)
                .set(XlakeOptions.STORAGE_MMAP_PATH, "/tmp/xlake/mmap")
                .set(XlakeOptions.STORAGE_MMAP_SIZE, 1024L * 1024L * 1024L)
                .build();
    }

    public static class Builder {
        private final Map<String, Object> values = new HashMap<>();

        public <T> Builder set(ConfigOption<T> option, T value) {
            values.put(option.key(), value);
            return this;
        }

        public <T> Builder setIfAbsent(ConfigOption<T> option, T value) {
            if (!values.containsKey(option.key())) {
                values.put(option.key(), value);
            }
            return this;
        }

        public XlakeConfig build() {
            return XlakeConfig.fromMap(values);
        }
    }
}
