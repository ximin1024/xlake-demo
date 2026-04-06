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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

@Slf4j
public class TableStore {

    @Getter
    private final String tableId;
    private final int writableNum;
    private final ConcurrentNavigableMap<String, LmdbInstance> readOnlyInstances = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<String, LmdbInstance> writableInstances = new ConcurrentSkipListMap<>();

    public TableStore(String tableId, int writableNum) {
        this.tableId = tableId;
        this.writableNum = writableNum;
    }

    public LmdbInstance getCurrentWritableInstance() {
        Map.Entry<String, LmdbInstance> entry = writableInstances.firstEntry();
        return entry == null ? null : entry.getValue();
    }

    public void addWritableInstance(LmdbInstance instance) {
        if (writableInstances.size() >= writableNum) {
            log.warn("Table[{}] writable instances limit reached ({}), reject add: {}",
                    tableId, writableNum, instance.instanceId());
            // 可以选择抛出异常或直接返回，这里选择返回
            return;
        }

        LmdbInstance prev = writableInstances.putIfAbsent(instance.instanceId(), instance);
        if (prev != null) {
            log.warn("Instance {} already exists in writable map", instance.instanceId());
        }
    }

    public void addReadOnlyInstance(LmdbInstance instance) {
        readOnlyInstances.put(instance.instanceId(), instance);
    }

    // 轮换可写实例：将最旧的实例从 writable 移至 readonly。
    public void rotateWritableInstance() {
        // pollFirstEntry 是原子操作，安全地移除并返回第一个元素
        Map.Entry<String, LmdbInstance> entry = writableInstances.pollFirstEntry();
        if (entry != null) {
            LmdbInstance old = entry.getValue();
            // 如果 instanceId 排序就是时间排序，则这里自然保持了顺序
            readOnlyInstances.put(entry.getKey(), old);
            log.info("Rotated instance {} to readonly", entry.getKey());
        }
    }

    public boolean isWritable() {
        return !writableInstances.isEmpty();
    }

    public int remainingWritable() {
        return writableNum - writableInstances.size();
    }

    public void forEachInstance(Consumer<LmdbInstance> consumer) {
        Objects.requireNonNull(consumer);
        readOnlyInstances.values().forEach(consumer);
        writableInstances.values().forEach(consumer);
    }

    public Stream<LmdbInstance> streamInstances() {
        return Stream.concat(
                readOnlyInstances.values().stream(),
                writableInstances.values().stream()
        );
    }

    public LmdbInstance getInstance(String instanceId) {
        LmdbInstance instance = writableInstances.get(instanceId);
        if (instance != null) {
            return instance;
        }
        return readOnlyInstances.get(instanceId);
    }

    // 按照 instanceId 倒序排列（最新的在前）。
    public List<LmdbInstance> getAllInstancesDescending() {
        List<LmdbInstance> all = new ArrayList<>();
        // descendingMap() 获取倒序视图
        all.addAll(writableInstances.descendingMap().values());
        all.addAll(readOnlyInstances.descendingMap().values());
        return all;
    }

    public List<LmdbInstance> getReadOnlyInstances() {
        return new ArrayList<>(readOnlyInstances.values());
    }

    public void removeInstance(LmdbInstance wrapper) {
        writableInstances.remove(wrapper.instanceId());
        readOnlyInstances.remove(wrapper.instanceId());
    }

    public void close() {
        getAllInstancesDescending().forEach(instance -> {
            try {
                instance.close();
            } catch (Exception ignored) {
            }
        });
    }
}
