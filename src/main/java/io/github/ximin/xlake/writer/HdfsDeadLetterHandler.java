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

import io.github.ximin.xlake.storage.table.record.KvRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

/**
 * 默认死信处理器：将写入失败的数据持久化到 HDFS
 * 数据格式：Hadoop SequenceFile (Key: Text/BytesWritable, Value: BytesWritable)
 */
@Slf4j
public class HdfsDeadLetterHandler implements DeadLetterHandler {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    private final FileSystem fs;
    private final String hdfsBasePath;
    private final Base64.Encoder base64Encoder = Base64.getEncoder();

    public HdfsDeadLetterHandler(String hdfsUri, String hdfsBasePath) {
        this.hdfsBasePath = hdfsBasePath;
        try {
            Configuration conf = new Configuration();
            // 如果需要，可以在这里设置 HDFS 用户权限代理
            // System.setProperty("HADOOP_USER_NAME", "hadoop_user");
            this.fs = FileSystem.get(URI.create(hdfsUri), conf);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize HDFS FileSystem", e);
        }
    }

    @Override
    public void save(List<KvRecord> failedData, Exception cause) {
        if (failedData == null || failedData.isEmpty()) {
            return;
        }
        // 1. 生成文件路径
        // 目录结构: base_path/instance_id(或default)/yyyy-MM-dd/uuid.seq
        // 这样方便按日期清理或重放
        String datePath = LocalDateTime.now().format(DATE_FORMATTER);
        String fileName = "dlq_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString().substring(0, 8) + ".seq";
        Path filePath = new Path(hdfsBasePath, datePath + "/" + fileName);
        SequenceFile.Writer writer = null;
        try {
            // 2. 创建 SequenceFile Writer
            // 使用 Snappy 压缩，节省空间
            SequenceFile.Writer.Option optPath = SequenceFile.Writer.file(filePath);
            SequenceFile.Writer.Option optKey = SequenceFile.Writer.keyClass(BytesWritable.class);
            SequenceFile.Writer.Option optVal = SequenceFile.Writer.valueClass(BytesWritable.class);
            SequenceFile.Writer.Option optCompress = SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new org.apache.hadoop.io.compress.SnappyCodec());
            writer = SequenceFile.createWriter(fs.getConf(), optPath, optKey, optVal, optCompress);
            // 3. 写入错误元数据
            // 将异常信息作为一条特殊的记录写入，Key 为 "ERROR_META"
            String errorMsg = String.format("Time: %s | Error: %s",
                    LocalDateTime.now(), cause.getMessage());
            writer.append(new BytesWritable("ERROR_META".getBytes()),
                    new BytesWritable(errorMsg.getBytes()));
            // 4. 批量写入数据
            // Key: 原始 Key (byte[])
            // Value: 原始 Value (byte[])
            BytesWritable keyWritable = new BytesWritable();
            BytesWritable valWritable = new BytesWritable();
            for (KvRecord kv : failedData) {
                // 注意：BytesWritable 会复用内部 buffer，需要 set
                keyWritable.set(kv.key(), 0, kv.key().length);
                valWritable.set(kv.value(), 0, kv.value().length);
                writer.append(keyWritable, valWritable);
            }
        } catch (Exception e) {
            // 如果连 HDFS 都挂了，只能打印日志
            // 生产环境建议降级写入本地磁盘或发送告警
            log.error("CRITICAL: Failed to write Dead Letter Queue to HDFS! Path: {}", filePath, e);
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}
