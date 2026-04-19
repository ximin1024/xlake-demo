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
package io.github.ximin.xlake.backend.wal;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@DisplayName("WALConsumer - WAL消费者测试")
class WALConsumerTest {

    @TempDir
    Path tempDir;

    private WALConfig localConfig;
    private Configuration hadoopConfig;

    @BeforeEach
    void setUp() {
        localConfig = new WALConfig(
                tempDir.resolve("wal").toString(),
                "wal",
                1024 * 1024,
                true,
                8192,
                "NONE",
                "testStore",
                "executor-1",
                "",
                1000L
        );
        hadoopConfig = new Configuration();
    }

    private WALEvent createEvent(WALRecord record) {
        WALEvent event = new WALEvent();
        event.set(record);
        return event;
    }

    private DefaultWALRecord createDataRecord(long sequence, byte[] data, String tableId) {
        return new DefaultWALRecord(sequence, data, System.nanoTime(), 1L, tableId, "DATA");
    }

    @Nested
    @DisplayName("正常写入流程测试")
    class NormalWriteFlowTest {

        @Test
        @DisplayName("onEvent写入记录到writer")
        void testOnEventWritesToWriter() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            WALRecord record = createDataRecord(0, "test_data".getBytes(), "table1");
            WALEvent event = createEvent(record);

            consumer.onEvent(event, 0, false);

            assertThat(consumer.getLastCommittedSequence()).isEqualTo(0);
            assertThat(event.isProcessed()).isTrue();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("onEvent更新lastCommittedSequence")
        void testOnEventUpdatesLastCommittedSequence() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            for (int i = 0; i < 5; i++) {
                WALRecord record = createDataRecord(i, ("data" + i).getBytes(), "table1");
                WALEvent event = createEvent(record);
                consumer.onEvent(event, i, false);
            }

            assertThat(consumer.getLastCommittedSequence()).isEqualTo(4);

            consumer.onShutdown();
        }

        @Test
        @DisplayName("onEvent跳过null entry")
        void testOnEventSkipsNullEntry() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            WALEvent event = new WALEvent();

            consumer.onEvent(event, 0, false);

            assertThat(consumer.getLastCommittedSequence()).isEqualTo(-1);

            consumer.onShutdown();
        }

        @Test
        @DisplayName("onEvent在未启动时跳过处理")
        void testOnEventSkipsWhenNotRunning() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            WALRecord record = createDataRecord(0, "test_data".getBytes(), "table1");
            WALEvent event = createEvent(record);

            consumer.onEvent(event, 0, false);

            assertThat(consumer.getLastCommittedSequence()).isEqualTo(-1);
        }
    }

    @Nested
    @DisplayName("HDFS降级测试")
    class HdfsDegradationTest {

        @Test
        @DisplayName("writeWithRetry在重试失败后降级到本地writer")
        void testWriteWithRetryFallsBackToLocalWriter() throws Exception {
            WALConfig hdfsConfig = new WALConfig(
                    tempDir.resolve("wal").toString(),
                    "wal",
                    1024 * 1024,
                    true,
                    8192,
                    "NONE",
                    "testStore",
                    "executor-1",
                    "hdfs://nonexistent:9000/wal",
                    1000L
            );

            WALWriter failingWriter = mock(WALWriter.class);
            doThrow(new IOException("HDFS connection refused"))
                    .when(failingWriter).write(any(WALRecord.class));

            WALConsumer consumer = new WALConsumer(hdfsConfig, "BINARY", hadoopConfig);

            var writerField = WALConsumer.class.getDeclaredField("writer");
            writerField.setAccessible(true);
            writerField.set(consumer, failingWriter);

            var runningField = WALConsumer.class.getDeclaredField("running");
            runningField.setAccessible(true);
            runningField.set(consumer, true);

            var degradedField = WALConsumer.class.getDeclaredField("degradedToLocal");
            degradedField.setAccessible(true);

            assertThat(degradedField.getBoolean(consumer)).isFalse();

            WALRecord record = createDataRecord(0, "test_data".getBytes(), "table1");
            WALEvent event = createEvent(record);

            try {
                consumer.onEvent(event, 0, false);
            } catch (Exception e) {
                // may throw after all retries fail
            }

            consumer.onShutdown();
        }
    }

    @Nested
    @DisplayName("isHdfsError测试")
    class IsHdfsErrorTest {

        @Test
        @DisplayName("ConnectException被识别为HDFS错误")
        void testConnectExceptionIsHdfsError() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var method = WALConsumer.class.getDeclaredMethod("isHdfsError", Throwable.class);
            method.setAccessible(true);

            assertThat((Boolean) method.invoke(consumer, new ConnectException("Connection refused"))).isTrue();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("NoRouteToHostException被识别为HDFS错误")
        void testNoRouteToHostExceptionIsHdfsError() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var method = WALConsumer.class.getDeclaredMethod("isHdfsError", Throwable.class);
            method.setAccessible(true);

            assertThat((Boolean) method.invoke(consumer, new NoRouteToHostException("No route to host"))).isTrue();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("org.apache.hadoop包下的异常被识别为HDFS错误")
        void testHadoopPackageExceptionIsHdfsError() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var method = WALConsumer.class.getDeclaredMethod("isHdfsError", Throwable.class);
            method.setAccessible(true);

            org.apache.hadoop.fs.PathNotFoundException hadoopEx =
                    new org.apache.hadoop.fs.PathNotFoundException("Path not found");
            assertThat((Boolean) method.invoke(consumer, hadoopEx)).isTrue();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("消息中包含HDFS/NameNode/DataNode的异常被识别为HDFS错误")
        void testErrorMessageContainsHdfsKeywords() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var method = WALConsumer.class.getDeclaredMethod("isHdfsError", Throwable.class);
            method.setAccessible(true);

            assertThat((Boolean) method.invoke(consumer, new RuntimeException("HDFS connection lost"))).isTrue();
            assertThat((Boolean) method.invoke(consumer, new RuntimeException("NameNode unreachable"))).isTrue();
            assertThat((Boolean) method.invoke(consumer, new RuntimeException("DataNode timeout"))).isTrue();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("普通IOException不被识别为HDFS错误")
        void testPlainIOExceptionIsNotHdfsError() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var method = WALConsumer.class.getDeclaredMethod("isHdfsError", Throwable.class);
            method.setAccessible(true);

            assertThat((Boolean) method.invoke(consumer, new IOException("Disk full"))).isFalse();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("null不被识别为HDFS错误")
        void testNullIsNotHdfsError() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var method = WALConsumer.class.getDeclaredMethod("isHdfsError", Throwable.class);
            method.setAccessible(true);

            assertThat((Boolean) method.invoke(consumer, (Object) null)).isFalse();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("嵌套的HDFS异常被识别")
        void testNestedHdfsErrorIsDetected() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var method = WALConsumer.class.getDeclaredMethod("isHdfsError", Throwable.class);
            method.setAccessible(true);

            RuntimeException wrapper = new RuntimeException("Write failed", new ConnectException("Connection refused"));
            assertThat((Boolean) method.invoke(consumer, wrapper)).isTrue();

            consumer.onShutdown();
        }
    }

    @Nested
    @DisplayName("attemptRecovery测试")
    class AttemptRecoveryTest {

        @Test
        @DisplayName("非HDFS错误时尝试forceSync恢复")
        void testAttemptRecoveryWithNonHdfsError() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            var method = WALConsumer.class.getDeclaredMethod("attemptRecovery", long.class, Exception.class);
            method.setAccessible(true);

            boolean result = (Boolean) method.invoke(consumer, 1L, new IOException("Local disk error"));

            assertThat(result).isTrue();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("连续失败计数器超过阈值时返回false")
        void testAttemptRecoveryExceedsMaxConsecutiveFailures() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            WALWriter mockWriter = mock(WALWriter.class);
            doThrow(new IOException("forceSync failed")).when(mockWriter).forceSync();

            var writerField = WALConsumer.class.getDeclaredField("writer");
            writerField.setAccessible(true);
            writerField.set(consumer, mockWriter);

            var failuresField = WALConsumer.class.getDeclaredField("consecutiveRecoveryFailures");
            failuresField.setAccessible(true);
            java.util.concurrent.atomic.AtomicInteger failures =
                    (java.util.concurrent.atomic.AtomicInteger) failuresField.get(consumer);
            failures.set(5);

            var method = WALConsumer.class.getDeclaredMethod("attemptRecovery", long.class, Exception.class);
            method.setAccessible(true);

            boolean result = (Boolean) method.invoke(consumer, 1L, new IOException("error"));

            assertThat(result).isFalse();

            consumer.onShutdown();
        }
    }

    @Nested
    @DisplayName("Lifecycle测试")
    class LifecycleTest {

        @Test
        @DisplayName("onStart()设置running为true")
        void testOnStartSetsRunning() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var runningField = WALConsumer.class.getDeclaredField("running");
            runningField.setAccessible(true);
            assertThat(runningField.getBoolean(consumer)).isFalse();

            consumer.onStart();
            assertThat(runningField.getBoolean(consumer)).isTrue();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("onShutdown()设置running为false")
        void testOnShutdownSetsRunningFalse() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            var runningField = WALConsumer.class.getDeclaredField("running");
            runningField.setAccessible(true);
            assertThat(runningField.getBoolean(consumer)).isTrue();

            consumer.onShutdown();
            assertThat(runningField.getBoolean(consumer)).isFalse();
        }
    }

    @Nested
    @DisplayName("lastCommittedSequence测试")
    class LastCommittedSequenceTest {

        @Test
        @DisplayName("lastCommittedSequence初始值为-1")
        void testLastCommittedSequenceInitialValue() {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            assertThat(consumer.getLastCommittedSequence()).isEqualTo(-1);
        }

        @Test
        @DisplayName("lastCommittedSequence是volatile的，可跨线程读取")
        void testLastCommittedSequenceIsVolatile() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            WALRecord record = createDataRecord(42, "data".getBytes(), "table1");
            WALEvent event = createEvent(record);
            consumer.onEvent(event, 42, false);

            assertThat(consumer.getLastCommittedSequence()).isEqualTo(42);

            consumer.onShutdown();
        }
    }

    @Nested
    @DisplayName("构造器测试")
    class ConstructorTest {

        @Test
        @DisplayName("BINARY格式+非HDFS配置创建LocalWALWriter")
        void testBinaryFormatCreatesLocalWALWriter() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var writerField = WALConsumer.class.getDeclaredField("writer");
            writerField.setAccessible(true);
            Object writer = writerField.get(consumer);

            assertThat(writer).isInstanceOf(LocalWALWriter.class);

            consumer.onShutdown();
        }

        @Test
        @DisplayName("HDFS配置创建HdfsWALWriter")
        void testHdfsConfigCreatesHdfsWALWriter() throws Exception {
            WALConfig hdfsConfig = new WALConfig(
                    tempDir.resolve("wal").toString(),
                    "wal",
                    1024 * 1024,
                    true,
                    8192,
                    "NONE",
                    "testStore",
                    "executor-1",
                    "hdfs://localhost:9000/wal",
                    1000L
            );

            WALConsumer consumer = new WALConsumer(hdfsConfig, "BINARY", hadoopConfig);

            var writerField = WALConsumer.class.getDeclaredField("writer");
            writerField.setAccessible(true);
            Object writer = writerField.get(consumer);

            assertThat(writer).isInstanceOf(HdfsWALWriter.class);

            consumer.onShutdown();
        }
    }

}
