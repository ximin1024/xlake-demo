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

import io.github.ximin.xlake.backend.writer.DefaultParquetWriterBuilder;
import io.github.ximin.xlake.common.Flusher;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.Iterator;

public class ParquetFlusher<T> implements Flusher {
    private final Iterator<T> iterator;
    private final ParquetWriter<T> fileWriter;

    public ParquetFlusher(Iterator<T> iterator, String filePath, WriteSupport<T> writeSupport) {
        this.iterator = iterator;
        DefaultParquetWriterBuilder<T> builder =
                new DefaultParquetWriterBuilder<>(new Path(filePath), writeSupport);
        try {
            this.fileWriter = builder.withBloomFilterEnabled(true)
                    .withDictionaryEncoding(true)
                    .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE)
                    .withCompressionCodec(CompressionCodecName.ZSTD)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() {
        iterator.forEachRemaining(element -> {
            try {
                fileWriter.write(element);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
