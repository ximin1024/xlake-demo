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
package io.github.ximin.xlake.backend.server.load;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@DisplayName("BackpressureManager Tests")
class BackpressureManagerTest {

    private LoadCollector mockLoadCollector;
    private BackpressureManager backpressureManager;

    @BeforeEach
    void setUp() {
        mockLoadCollector = mock(LoadCollector.class);
        backpressureManager = new BackpressureManager(mockLoadCollector);
    }

    @Test
    @DisplayName("getCurrentLoad should delegate to LoadCollector")
    void getCurrentLoad_shouldDelegateToLoadCollector() {
        BackendLoadInfo expectedLoad = BackendLoadInfo.builder()
                .queueLength(10)
                .cpuUsage(0.5)
                .memoryUsage(0.4)
                .diskIoUsage(0.3)
                .networkUsage(0.2)
                .build();
        when(mockLoadCollector.collect()).thenReturn(expectedLoad);

        BackendLoadInfo result = backpressureManager.getCurrentLoad();

        verify(mockLoadCollector, times(1)).collect();
        assertThat(result).isEqualTo(expectedLoad);
    }

    @Test
    @DisplayName("shouldReject returns false when load is below reject threshold")
    void shouldReject_whenLoadBelowRejectThreshold() {
        BackendLoadInfo lowLoad = BackendLoadInfo.builder()
                .queueLength(10)
                .cpuUsage(0.1)
                .memoryUsage(0.1)
                .diskIoUsage(0.0)
                .networkUsage(0.0)
                .build();
        when(mockLoadCollector.collect()).thenReturn(lowLoad);

        assertThat(backpressureManager.shouldReject()).isFalse();
    }

    @Test
    @DisplayName("shouldReject returns true when load equals or exceeds reject threshold")
    void shouldReject_whenLoadExceedsRejectThreshold() {
        BackendLoadInfo highLoad = BackendLoadInfo.builder()
                .queueLength(100)
                .cpuUsage(1.0)
                .memoryUsage(1.0)
                .diskIoUsage(1.0)
                .networkUsage(1.0)
                .build();
        when(mockLoadCollector.collect()).thenReturn(highLoad);

        assertThat(backpressureManager.shouldReject()).isTrue();
    }

    @Test
    @DisplayName("shouldThrottle returns false when load is below throttle threshold")
    void shouldThrottle_whenLoadBelowThrottleThreshold() {
        BackendLoadInfo lowLoad = BackendLoadInfo.builder()
                .queueLength(30)
                .cpuUsage(0.3)
                .memoryUsage(0.3)
                .diskIoUsage(0.0)
                .networkUsage(0.0)
                .build();
        when(mockLoadCollector.collect()).thenReturn(lowLoad);

        assertThat(backpressureManager.shouldThrottle()).isFalse();
    }

    @Test
    @DisplayName("shouldThrottle returns true when load equals or exceeds throttle threshold")
    void shouldThrottle_whenLoadExceedsThrottleThreshold() {
        BackendLoadInfo highLoad = BackendLoadInfo.builder()
                .queueLength(100)
                .cpuUsage(1.0)
                .memoryUsage(1.0)
                .diskIoUsage(1.0)
                .networkUsage(1.0)
                .build();
        when(mockLoadCollector.collect()).thenReturn(highLoad);

        assertThat(backpressureManager.shouldThrottle()).isTrue();
    }

    @Test
    @DisplayName("shouldWarn returns false when load is below warning threshold")
    void shouldWarn_whenLoadBelowWarningThreshold() {
        BackendLoadInfo lowLoad = BackendLoadInfo.builder()
                .queueLength(20)
                .cpuUsage(0.2)
                .memoryUsage(0.2)
                .diskIoUsage(0.0)
                .networkUsage(0.0)
                .build();
        when(mockLoadCollector.collect()).thenReturn(lowLoad);

        assertThat(backpressureManager.shouldWarn()).isFalse();
    }

    @Test
    @DisplayName("shouldWarn returns true when load is between warning and throttle thresholds")
    void shouldWarn_whenLoadInWarningRange() {
        BackendLoadInfo warningLoad = BackendLoadInfo.builder()
                .queueLength(95)
                .cpuUsage(0.95)
                .memoryUsage(0.95)
                .diskIoUsage(0.0)
                .networkUsage(0.0)
                .build();
        when(mockLoadCollector.collect()).thenReturn(warningLoad);

        assertThat(backpressureManager.shouldWarn()).isTrue();
    }

    @Test
    @DisplayName("getStatus returns NORMAL when load is below warning threshold")
    void getStatus_whenLoadBelowWarningThreshold() {
        BackendLoadInfo lowLoad = BackendLoadInfo.builder()
                .queueLength(10)
                .cpuUsage(0.1)
                .memoryUsage(0.1)
                .diskIoUsage(0.0)
                .networkUsage(0.0)
                .build();
        when(mockLoadCollector.collect()).thenReturn(lowLoad);

        assertThat(backpressureManager.getStatus())
                .isEqualTo(BackpressureManager.BackpressureStatus.NORMAL);
    }

    @Test
    @DisplayName("getStatus returns WARNING when load is between warning and throttle thresholds")
    void getStatus_whenLoadInWarningRange() {
        BackendLoadInfo warningLoad = BackendLoadInfo.builder()
                .queueLength(95)
                .cpuUsage(0.95)
                .memoryUsage(0.95)
                .diskIoUsage(0.0)
                .networkUsage(0.0)
                .build();
        when(mockLoadCollector.collect()).thenReturn(warningLoad);

        assertThat(backpressureManager.getStatus())
                .isEqualTo(BackpressureManager.BackpressureStatus.WARNING);
    }

    @Test
    @DisplayName("getStatus returns THROTTLING when load is between throttle and reject thresholds")
    void getStatus_whenLoadInThrottleRange() {
        BackendLoadInfo throttleLoad = BackendLoadInfo.builder()
                .queueLength(100)
                .cpuUsage(0.82)
                .memoryUsage(0.82)
                .diskIoUsage(0.82)
                .networkUsage(0.82)
                .build();
        when(mockLoadCollector.collect()).thenReturn(throttleLoad);

        assertThat(backpressureManager.getStatus())
                .isEqualTo(BackpressureManager.BackpressureStatus.THROTTLING);
    }

    @Test
    @DisplayName("getStatus returns REJECTING when load equals or exceeds reject threshold")
    void getStatus_whenLoadExceedsRejectThreshold() {
        BackendLoadInfo highLoad = BackendLoadInfo.builder()
                .queueLength(100)
                .cpuUsage(1.0)
                .memoryUsage(1.0)
                .diskIoUsage(1.0)
                .networkUsage(1.0)
                .build();
        when(mockLoadCollector.collect()).thenReturn(highLoad);

        assertThat(backpressureManager.getStatus())
                .isEqualTo(BackpressureManager.BackpressureStatus.REJECTING);
    }

    @Test
    @DisplayName("BackpressureStatus enum should have all expected values")
    void backpressureStatus_shouldHaveAllExpectedValues() {
        BackpressureManager.BackpressureStatus[] values =
                BackpressureManager.BackpressureStatus.values();

        assertThat(values).containsExactlyInAnyOrder(
                BackpressureManager.BackpressureStatus.NORMAL,
                BackpressureManager.BackpressureStatus.WARNING,
                BackpressureManager.BackpressureStatus.THROTTLING,
                BackpressureManager.BackpressureStatus.REJECTING
        );
    }
}
