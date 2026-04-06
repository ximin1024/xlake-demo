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
package io.github.ximin.xlake.writer;

import io.github.ximin.xlake.backend.LmdbInstance;

import java.nio.file.Path;
import java.util.Properties;

public final class WriterFactory {

    private WriterFactory() {
    }

    @SuppressWarnings("unchecked")
    public static <T> Writer createWriter(WriterType writerType, Properties config) {
        if (writerType == null) {
            throw new IllegalArgumentException("WriterType cannot be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("Config cannot be null");
        }

        try {
            Writer writer = writerType.create(config);

            writer.init(config);

            return writer;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create writer for type: " + writerType, e);
        }
    }

    public static <T> Writer createWriter(Properties config) {
        String typeStr = config.getProperty("writer.type");
        if (typeStr == null) {
            throw new IllegalArgumentException("Property 'writer.type' is missing");
        }

        try {
            WriterType writerType = WriterType.valueOf(typeStr.toUpperCase());
            return createWriter(writerType, config);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown writer type: " + typeStr, e);
        }
    }

    public enum WriterType {

        LMDB {
            @Override
            public Writer<?> create(Properties config) {
                String path = config.getProperty("lmdb.path", "/tmp/default-lmdb");
                String tableIdentifier = config.getProperty("table.identifier");
                long mapSize = Long.parseLong(config.getProperty("lmdb.mapSize", "1073741824"));
                LmdbInstance instance = new LmdbInstance(LmdbInstance.generateInstanceId(tableIdentifier), Path.of(path), mapSize);
                return new LmdbWriter(instance);
            }
        },

        PARQUET {
            @Override
            public Writer<?> create(Properties config) {
                // ParquetWriter 继承自 FileWriter
                //return new ParquetWriter();
                return null;
            }
        };

        public abstract Writer<?> create(Properties config);
    }
}
