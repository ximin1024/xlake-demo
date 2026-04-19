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

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("WAL - Disruptor耐久性改进测试")
class WALTest {

    @TempDir
    Path tempDir;

    private WALConfig createConfig() {
        return new WALConfig(
                tempDir.resolve("wal").toString(),
                "wal",
                1024 * 1024,
                true,
                8192,
                "NONE",
                "testStore",
                "executor-1",
                "",
                1000L,
                30000L
        );
    }

    private WALConfig createConfig(String subdir) {
        return new WALConfig(
                tempDir.resolve(subdir).toString(),
                "wal",
                1024 * 1024,
                true,
                8192,
                "NONE",
                "testStore",
                "executor-1",
                "",
                1000L,
                30000L
        );
    }

    private WALConfig createConfig(String subdir, long appendTimeoutMs) {
        return new WALConfig(
                tempDir.resolve(subdir).toString(),
                "wal",
                1024 * 1024,
                true,
                8192,
                "NONE",
                "testStore",
                "executor-1",
                "",
                1000L,
                appendTimeoutMs
        );
    }

    private WALRecord createTestRecord() {
        return new DefaultWALRecord(0, "test_data".getBytes(), System.nanoTime(), 1L, "table1", "DATA");
    }

    private void waitForConsumerRunning(WAL wal) throws Exception {
        var consumerField = WAL.class.getDeclaredField("consumer");
        consumerField.setAccessible(true);
        WALConsumer consumer = (WALConsumer) consumerField.get(wal);

        long deadline = System.currentTimeMillis() + 5000;
        while (!consumer.isRunning() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(consumer.isRunning()).isTrue();
    }

    @Nested
    @DisplayName("append()正常流程测试")
    class AppendNormalTest {

        @Test
        @DisplayName("append()正常写入成功")
        void testAppendSucceedsNormally() throws Exception {
            WAL wal = new WAL(createConfig("normal"));
            try {
                waitForConsumerRunning(wal);
                assertThat(wal.isRunning()).isTrue();

                long seq = wal.append(createTestRecord(), false);

                assertThat(seq).isGreaterThanOrEqualTo(0);
            } finally {
                wal.close();
            }
        }

        @Test
        @DisplayName("append()带sync写入成功")
        void testAppendWithSyncSucceeds() throws Exception {
            WAL wal = new WAL(createConfig("sync"));
            try {
                waitForConsumerRunning(wal);

                long seq = wal.append(createTestRecord(), true);

                assertThat(seq).isGreaterThanOrEqualTo(0);
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("append()关闭状态测试")
    class AppendClosedTest {

        @Test
        @DisplayName("append()在WAL关闭后抛出WALWriteException")
        void testAppendThrowsAfterClose() throws Exception {
            WAL wal = new WAL(createConfig("closed"));
            waitForConsumerRunning(wal);
            wal.close();

            assertThatThrownBy(() -> wal.append(createTestRecord(), false))
                    .isInstanceOf(WALWriteException.class)
                    .hasMessageContaining("not running");
        }

        @Test
        @DisplayName("append()在WAL关闭后带sync也抛出WALWriteException")
        void testAppendWithSyncThrowsAfterClose() throws Exception {
            WAL wal = new WAL(createConfig("closed_sync"));
            waitForConsumerRunning(wal);
            wal.close();

            assertThatThrownBy(() -> wal.append(createTestRecord(), true))
                    .isInstanceOf(WALWriteException.class);
        }
    }

    @Nested
    @DisplayName("append() shutdownLock测试")
    class AppendShutdownLockTest {

        @Test
        @DisplayName("append()在shutdownLock被持有时抛出WALWriteException")
        void testAppendThrowsDuringShutdown() throws Exception {
            WAL wal = new WAL(createConfig("shutdown_lock"));
            try {
                var shutdownLockField = WAL.class.getDeclaredField("shutdownLock");
                shutdownLockField.setAccessible(true);
                java.util.concurrent.locks.ReentrantLock shutdownLock =
                        (java.util.concurrent.locks.ReentrantLock) shutdownLockField.get(wal);

                CountDownLatch lockHeld = new CountDownLatch(1);
                CountDownLatch testDone = new CountDownLatch(1);

                Thread lockHolder = new Thread(() -> {
                    shutdownLock.lock();
                    try {
                        lockHeld.countDown();
                        testDone.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        shutdownLock.unlock();
                    }
                });
                lockHolder.start();

                lockHeld.await(5, TimeUnit.SECONDS);

                assertThatThrownBy(() -> wal.append(createTestRecord(), false))
                        .isInstanceOf(WALWriteException.class)
                        .hasMessage("WAL is shutting down");

                testDone.countDown();
                lockHolder.join(5000);
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("append() consumer未运行测试")
    class AppendConsumerNotRunningTest {

        @Test
        @DisplayName("append()在consumer未运行时抛出WALWriteException")
        void testAppendThrowsWhenConsumerNotRunning() throws Exception {
            WAL wal = new WAL(createConfig("consumer_stop"));
            try {
                waitForConsumerRunning(wal);

                var consumerField = WAL.class.getDeclaredField("consumer");
                consumerField.setAccessible(true);
                WALConsumer consumer = (WALConsumer) consumerField.get(wal);

                consumer.onShutdown();

                assertThatThrownBy(() -> wal.append(createTestRecord(), false))
                        .isInstanceOf(WALWriteException.class)
                        .hasMessageContaining("not running");
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("insertSyncMarker()测试")
    class InsertSyncMarkerTest {

        @Test
        @DisplayName("insertSyncMarker()正常执行成功")
        void testInsertSyncMarkerSucceedsNormally() throws Exception {
            WAL wal = new WAL(createConfig("sync_marker"));
            try {
                waitForConsumerRunning(wal);

                assertThatCode(() -> wal.insertSyncMarker("table1"))
                        .doesNotThrowAnyException();
            } finally {
                wal.close();
            }
        }

        @Test
        @DisplayName("insertSyncMarker()在WAL关闭后抛出WALWriteException")
        void testInsertSyncMarkerThrowsAfterClose() throws Exception {
            WAL wal = new WAL(createConfig("sync_marker_closed"));
            waitForConsumerRunning(wal);
            wal.close();

            assertThatThrownBy(() -> wal.insertSyncMarker("table1"))
                    .isInstanceOf(WALWriteException.class)
                    .hasMessageContaining("not running");
        }

        @Test
        @DisplayName("insertSyncMarker()在shutdownLock被持有时抛出WALWriteException")
        void testInsertSyncMarkerThrowsDuringShutdown() throws Exception {
            WAL wal = new WAL(createConfig("sync_marker_shutdown"));
            try {
                var shutdownLockField = WAL.class.getDeclaredField("shutdownLock");
                shutdownLockField.setAccessible(true);
                java.util.concurrent.locks.ReentrantLock shutdownLock =
                        (java.util.concurrent.locks.ReentrantLock) shutdownLockField.get(wal);

                CountDownLatch lockHeld = new CountDownLatch(1);
                CountDownLatch testDone = new CountDownLatch(1);

                Thread lockHolder = new Thread(() -> {
                    shutdownLock.lock();
                    try {
                        lockHeld.countDown();
                        testDone.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        shutdownLock.unlock();
                    }
                });
                lockHolder.start();

                lockHeld.await(5, TimeUnit.SECONDS);

                assertThatThrownBy(() -> wal.insertSyncMarker("table1"))
                        .isInstanceOf(WALWriteException.class)
                        .hasMessage("WAL is shutting down");

                testDone.countDown();
                lockHolder.join(5000);
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("isRunning()测试")
    class IsRunningTest {

        @Test
        @DisplayName("isRunning()在running和consumer都运行时返回true")
        void testIsRunningReturnsTrueWhenBothRunning() throws Exception {
            WAL wal = new WAL(createConfig("is_running"));
            try {
                waitForConsumerRunning(wal);

                assertThat(wal.isRunning()).isTrue();
            } finally {
                wal.close();
            }
        }

        @Test
        @DisplayName("isRunning()在WAL关闭后返回false")
        void testIsRunningReturnsFalseAfterClose() throws Exception {
            WAL wal = new WAL(createConfig("is_running_closed"));
            waitForConsumerRunning(wal);
            wal.close();

            assertThat(wal.isRunning()).isFalse();
        }

        @Test
        @DisplayName("isRunning()在consumer停止后返回false")
        void testIsRunningReturnsFalseWhenConsumerStopped() throws Exception {
            WAL wal = new WAL(createConfig("is_running_consumer"));
            try {
                waitForConsumerRunning(wal);

                var consumerField = WAL.class.getDeclaredField("consumer");
                consumerField.setAccessible(true);
                WALConsumer consumer = (WALConsumer) consumerField.get(wal);

                consumer.onShutdown();

                assertThat(wal.isRunning()).isFalse();
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("notifyConsumerStopped()测试")
    class NotifyConsumerStoppedTest {

        @Test
        @DisplayName("notifyConsumerStopped()将running设为false")
        void testNotifyConsumerStoppedSetsRunningFalse() throws Exception {
            WAL wal = new WAL(createConfig("notify_stop"));
            try {
                var runningField = WAL.class.getDeclaredField("running");
                runningField.setAccessible(true);
                AtomicBoolean running =
                        (AtomicBoolean) runningField.get(wal);

                assertThat(running.get()).isTrue();

                var method = WAL.class.getDeclaredMethod("notifyConsumerStopped");
                method.setAccessible(true);
                method.invoke(wal);

                assertThat(running.get()).isFalse();
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("close()幂等性测试")
    class CloseIdempotentTest {

        @Test
        @DisplayName("close()是幂等的，多次调用不抛异常")
        void testCloseIsIdempotent() throws Exception {
            WAL wal = new WAL(createConfig("idempotent"));
            waitForConsumerRunning(wal);

            assertThatCode(() -> {
                wal.close();
                wal.close();
                wal.close();
            }).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("tryNextWithRetry()测试")
    class TryNextWithRetryTest {

        @Test
        @DisplayName("tryNextWithRetry()在InsufficientCapacityException时重试")
        void testTryNextWithRetryRetriesOnInsufficientCapacity() throws Exception {
            WAL wal = new WAL(createConfig("retry"));
            try {
                RingBuffer<WALEvent> mockRingBuffer = mock(RingBuffer.class);
                when(mockRingBuffer.tryNext())
                        .thenThrow(InsufficientCapacityException.INSTANCE)
                        .thenThrow(InsufficientCapacityException.INSTANCE)
                        .thenReturn(42L);
                when(mockRingBuffer.get(42L)).thenReturn(new WALEvent());

                var ringBufferField = WAL.class.getDeclaredField("ringBuffer");
                ringBufferField.setAccessible(true);
                ringBufferField.set(wal, mockRingBuffer);

                var method = WAL.class.getDeclaredMethod("tryNextWithRetry");
                method.setAccessible(true);

                long result = (long) method.invoke(wal);

                assertThat(result).isEqualTo(42L);
                verify(mockRingBuffer, times(3)).tryNext();
            } finally {
                wal.close();
            }
        }

        @Test
        @DisplayName("tryNextWithRetry()超过最大重试次数后抛出WALWriteException")
        void testTryNextWithRetryThrowsAfterMaxRetries() throws Exception {
            WAL wal = new WAL(createConfig("retry_fail"));
            try {
                RingBuffer<WALEvent> mockRingBuffer = mock(RingBuffer.class);
                when(mockRingBuffer.tryNext())
                        .thenThrow(InsufficientCapacityException.INSTANCE);

                var ringBufferField = WAL.class.getDeclaredField("ringBuffer");
                ringBufferField.setAccessible(true);
                ringBufferField.set(wal, mockRingBuffer);

                var method = WAL.class.getDeclaredMethod("tryNextWithRetry");
                method.setAccessible(true);

                Throwable thrown = catchThrowable(() -> method.invoke(wal));

                assertThat(thrown.getCause()).isInstanceOf(WALWriteException.class);
                assertThat(thrown.getCause().getMessage()).contains("ring buffer is full");
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("waitForSequence()测试")
    class WaitForSequenceTest {

        @Test
        @DisplayName("waitForSequence()超时时抛出WALWriteException")
        void testWaitForSequenceThrowsOnTimeout() throws Exception {
            WALConfig shortTimeoutConfig = createConfig("timeout", 100L);

            WAL wal = new WAL(shortTimeoutConfig);
            try {
                var consumerField = WAL.class.getDeclaredField("consumer");
                consumerField.setAccessible(true);
                WALConsumer consumer = (WALConsumer) consumerField.get(wal);

                var method = WAL.class.getDeclaredMethod("waitForSequence", long.class);
                method.setAccessible(true);

                long farSequence = consumer.getLastCommittedSequence() + 100000L;

                Throwable thrown = catchThrowable(() -> method.invoke(wal, farSequence));

                assertThat(thrown.getCause()).isInstanceOf(WALWriteException.class);
                assertThat(thrown.getCause().getMessage()).contains("Timeout waiting for sequence");
            } finally {
                wal.close();
            }
        }

        @Test
        @DisplayName("waitForSequence()在WAL停止时抛出WALWriteException")
        void testWaitForSequenceThrowsWhenWalStops() throws Exception {
            WAL wal = new WAL(createConfig("wait_stop"));
            try {
                var runningField = WAL.class.getDeclaredField("running");
                runningField.setAccessible(true);
                AtomicBoolean running =
                        (AtomicBoolean) runningField.get(wal);

                var consumerField = WAL.class.getDeclaredField("consumer");
                consumerField.setAccessible(true);
                WALConsumer consumer = (WALConsumer) consumerField.get(wal);

                running.set(false);

                var method = WAL.class.getDeclaredMethod("waitForSequence", long.class);
                method.setAccessible(true);

                long farSequence = consumer.getLastCommittedSequence() + 100000L;

                Throwable thrown = catchThrowable(() -> method.invoke(wal, farSequence));

                assertThat(thrown.getCause()).isInstanceOf(WALWriteException.class);
                assertThat(thrown.getCause().getMessage()).contains("stopped while waiting for sequence");
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("Daemon线程验证测试")
    class DaemonThreadTest {

        @Test
        @DisplayName("Disruptor线程是daemon线程")
        void testDisruptorThreadsAreDaemon() throws Exception {
            WAL wal = new WAL(createConfig("daemon"));
            try {
                boolean foundDaemonWalThread = false;
                long deadline = System.currentTimeMillis() + 5000;
                while (!foundDaemonWalThread && System.currentTimeMillis() < deadline) {
                    for (Thread t : Thread.getAllStackTraces().keySet()) {
                        if (t.getName().equals("wal-disruptor")) {
                            assertThat(t.isDaemon()).isTrue();
                            foundDaemonWalThread = true;
                            break;
                        }
                    }
                    if (!foundDaemonWalThread) {
                        Thread.sleep(50);
                    }
                }
                assertThat(foundDaemonWalThread)
                        .withFailMessage("Expected to find a daemon thread named 'wal-disruptor'")
                        .isTrue();
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("WALExceptionHandler验证测试")
    class ExceptionHandlerTest {

        @Test
        @DisplayName("Disruptor使用WALExceptionHandler而非FatalExceptionHandler")
        void testDisruptorUsesWALExceptionHandler() throws Exception {
            WAL wal = new WAL(createConfig("ex_handler"));
            try {
                var disruptorField = WAL.class.getDeclaredField("disruptor");
                disruptorField.setAccessible(true);
                com.lmax.disruptor.dsl.Disruptor<WALEvent> disruptor =
                        (com.lmax.disruptor.dsl.Disruptor<WALEvent>) disruptorField.get(wal);

                var exceptionHandlerField = com.lmax.disruptor.dsl.Disruptor.class.getDeclaredField("exceptionHandler");
                exceptionHandlerField.setAccessible(true);
                Object exceptionHandler = exceptionHandlerField.get(disruptor);

                assertThat(exceptionHandler.getClass().getName()).contains("ExceptionHandlerWrapper");

                var delegateField = exceptionHandler.getClass().getDeclaredField("delegate");
                delegateField.setAccessible(true);
                Object delegate = delegateField.get(exceptionHandler);

                assertThat(delegate).isInstanceOf(WALExceptionHandler.class);
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("running.set(true)在disruptor.start()之后测试")
    class RunningSetAfterStartTest {

        @Test
        @DisplayName("WAL构造完成后running为true，表示start()已成功执行")
        void testRunningIsTrueAfterConstruction() throws Exception {
            WAL wal = new WAL(createConfig("after_start"));
            try {
                var runningField = WAL.class.getDeclaredField("running");
                runningField.setAccessible(true);
                AtomicBoolean running =
                        (AtomicBoolean) runningField.get(wal);

                assertThat(running.get()).isTrue();
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("appendTimeoutMs配置测试")
    class AppendTimeoutConfigTest {

        @Test
        @DisplayName("appendTimeoutMs从WALConfig正确传递到WAL")
        void testAppendTimeoutMsFromConfig() throws Exception {
            WALConfig customConfig = createConfig("custom_timeout", 5000L);

            WAL wal = new WAL(customConfig);
            try {
                var appendTimeoutField = WAL.class.getDeclaredField("appendTimeoutMs");
                appendTimeoutField.setAccessible(true);
                long appendTimeout = (long) appendTimeoutField.get(wal);

                assertThat(appendTimeout).isEqualTo(5000L);
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("并发append与close测试")
    class ConcurrentAppendCloseTest {

        @Test
        @DisplayName("并发append和close不会产生数据竞争异常")
        void testConcurrentAppendAndClose() throws Exception {
            WAL wal = new WAL(createConfig("concurrent"));
            waitForConsumerRunning(wal);

            int threadCount = 4;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicBoolean hasUnexpectedException = new AtomicBoolean(false);

            for (int i = 0; i < threadCount; i++) {
                final int idx = i;
                new Thread(() -> {
                    try {
                        startLatch.await();
                        if (idx == 0) {
                            Thread.sleep(10);
                            wal.close();
                        } else {
                            wal.append(createTestRecord(), false);
                        }
                    } catch (WALWriteException e) {
                        // expected when WAL is closed
                    } catch (Exception e) {
                        hasUnexpectedException.set(true);
                    } finally {
                        doneLatch.countDown();
                    }
                }).start();
            }

            startLatch.countDown();
            doneLatch.await(10, TimeUnit.SECONDS);

            assertThat(hasUnexpectedException.get()).isFalse();
        }
    }
}
