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
package io.github.ximin.xlake.storage.flush;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ParquetHdfsUploader {
    private final String hdfsBasePath;
    private final Configuration hadoopConf;
    private final ExecutorService uploadExecutor;

    public ParquetHdfsUploader(String hdfsBasePath, Configuration hadoopConf) {
        this.hdfsBasePath = hdfsBasePath;
        this.hadoopConf = hadoopConf;
        this.uploadExecutor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "parquet-hdfs-uploader");
            t.setDaemon(true);
            return t;
        });
    }

    public CompletableFuture<Boolean> uploadAsync(String localFilePath, String tableId, String fileName) {
        return CompletableFuture.supplyAsync(() -> upload(localFilePath, tableId, fileName), uploadExecutor);
    }

    public boolean upload(String localFilePath, String tableId, String fileName) {
        if (hdfsBasePath == null || hdfsBasePath.isBlank()) {
            log.debug("HDFS base path not configured, skipping upload for {}", fileName);
            return true;
        }

        String targetPath = hdfsBasePath + "/" + tableId + "/" + fileName;
        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            Path hdfsPath = new Path(targetPath);
            Path localPath = new Path(localFilePath);

            fs.mkdirs(hdfsPath.getParent());
            fs.copyFromLocalFile(false, true, localPath, hdfsPath);
            log.info("Uploaded Parquet file to HDFS: {}", targetPath);
            return true;
        } catch (IOException e) {
            log.error("Failed to upload Parquet file to HDFS: {}", targetPath, e);
            return false;
        }
    }

    public boolean uploadFromLocal(java.nio.file.Path localPath, String tableId, String fileName) {
        if (hdfsBasePath == null || hdfsBasePath.isBlank()) {
            return true;
        }
        if (!Files.exists(localPath)) {
            log.warn("Local file does not exist for upload: {}", localPath);
            return false;
        }
        return upload(localPath.toString(), tableId, fileName);
    }

    public void close() {
        uploadExecutor.shutdown();
    }
}
