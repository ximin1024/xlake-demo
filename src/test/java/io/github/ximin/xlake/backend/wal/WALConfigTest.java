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

import io.github.ximin.xlake.common.config.XlakeOptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WALConfig配置选项测试
 *
 * 测试要点:
 * 1. STORAGE_WAL_ENABLED 默认值为 true
 * 2. STORAGE_WAL_BUFFER_SIZE 默认值为 8192
 * 3. STORAGE_WAL_SYNC_INTERVAL_MS 默认值为 1000L
 * 4. STORAGE_WAL_MAX_SIZE_BYTES 默认值为 100MB
 * 5. WALConfig.DEFAULT 默认值正确
 */
@DisplayName("WALConfig - 配置选项测试")
class WALConfigTest {

    @Nested
    @DisplayName("WALConfig.DEFAULT默认值测试")
    class DefaultValuesTest {

        @Test
        @DisplayName("WALConfig.DEFAULT的logDir默认值为'wal_logs'")
        void testDefaultLogDir() {
            assertThat(WALConfig.DEFAULT.logDir()).isEqualTo("wal_logs");
        }

        @Test
        @DisplayName("WALConfig.DEFAULT的logFilePrefix默认值为'wal'")
        void testDefaultLogFilePrefix() {
            assertThat(WALConfig.DEFAULT.logFilePrefix()).isEqualTo("wal");
        }

        @Test
        @DisplayName("WALConfig.DEFAULT的segmentSize默认值为100MB")
        void testDefaultSegmentSize() {
            assertThat(WALConfig.DEFAULT.segmentSize()).isEqualTo(100 * 1024 * 1024);
        }

        @Test
        @DisplayName("WALConfig.DEFAULT的syncOnWrite默认值为true")
        void testDefaultSyncOnWrite() {
            assertThat(WALConfig.DEFAULT.syncOnWrite()).isTrue();
        }

        @Test
        @DisplayName("WALConfig.DEFAULT的bufferSize默认值为8192")
        void testDefaultBufferSize() {
            assertThat(WALConfig.DEFAULT.bufferSize()).isEqualTo(8192);
        }
    }

    @Nested
    @DisplayName("WALConfig构造器测试")
    class ConstructorTest {

        @Test
        @DisplayName("WALConfig可以使用自定义值创建")
        void testCustomWALConfig() {
            WALConfig config = new WALConfig(
                    "/custom/wal/path",
                    "custom_prefix",
                    50 * 1024 * 1024, // 50MB
                    false,
                    4096
            );

            assertThat(config.logDir()).isEqualTo("/custom/wal/path");
            assertThat(config.logFilePrefix()).isEqualTo("custom_prefix");
            assertThat(config.segmentSize()).isEqualTo(50 * 1024 * 1024);
            assertThat(config.syncOnWrite()).isFalse();
            assertThat(config.bufferSize()).isEqualTo(4096);
        }

        @Test
        @DisplayName("WALConfig是record类型，支持equals和hashCode")
        void testWALConfigEquality() {
            WALConfig config1 = new WALConfig("dir", "prefix", 100L, true, 8192);
            WALConfig config2 = new WALConfig("dir", "prefix", 100L, true, 8192);

            assertThat(config1).isEqualTo(config2);
            assertThat(config1.hashCode()).isEqualTo(config2.hashCode());
        }
    }

    @Nested
    @DisplayName("XlakeOptions WAL配置选项测试")
    class XlakeOptionsWALTest {

        @Test
        @DisplayName("STORAGE_WAL_ENABLED默认值为true")
        void testStorageWalEnabledDefault() {
            assertThat(XlakeOptions.STORAGE_WAL_ENABLED.defaultValue()).isTrue();
        }

        @Test
        @DisplayName("STORAGE_WAL_BUFFER_SIZE默认值为8192")
        void testStorageWalBufferSizeDefault() {
            assertThat(XlakeOptions.STORAGE_WAL_BUFFER_SIZE.defaultValue()).isEqualTo(8192);
        }

        @Test
        @DisplayName("STORAGE_WAL_SYNC_INTERVAL_MS默认值为1000L")
        void testStorageWalSyncIntervalMsDefault() {
            assertThat(XlakeOptions.STORAGE_WAL_SYNC_INTERVAL_MS.defaultValue()).isEqualTo(1000L);
        }

        @Test
        @DisplayName("STORAGE_WAL_MAX_SIZE_BYTES默认值为100MB")
        void testStorageWalMaxSizeBytesDefault() {
            assertThat(XlakeOptions.STORAGE_WAL_MAX_SIZE_BYTES.defaultValue()).isEqualTo(100L * 1024 * 1024);
        }
    }
}
