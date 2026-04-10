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
package io.github.ximin.xlake.table.keygen;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TimestampKeyGenerator implements KeyGenerator {

    private final ConcurrentMap<Integer, PartitionState> stateMap = new ConcurrentHashMap<>();

    private static class PartitionState {
        private final AtomicLong lastTimestamp = new AtomicLong(-1);
        private final AtomicInteger seq = new AtomicInteger(0);
    }

    @Override
    public byte[] generateKey(int partitionId) {
        PartitionState state = stateMap.computeIfAbsent(partitionId, k -> new PartitionState());
        long now = System.currentTimeMillis();
        long last = state.lastTimestamp.get();

        if (now < last) {
            // 时间回拨，等待或抛出异常（此处采用等待直到超过上次时间）
            while (now < last) {
                now = System.currentTimeMillis();
            }
        }

        int sequence;
        if (now == last) {
            // 同一毫秒内，序列号递增
            sequence = state.seq.incrementAndGet();
        } else {
            // 新的一毫秒，重置序列号
            state.seq.set(0);
            sequence = 0;
            state.lastTimestamp.set(now);
        }

        // 编码为 16 字节大端序
        byte[] key = new byte[16];
        // 时间戳 8 字节
        key[0] = (byte) (now >>> 56);
        key[1] = (byte) (now >>> 48);
        key[2] = (byte) (now >>> 40);
        key[3] = (byte) (now >>> 32);
        key[4] = (byte) (now >>> 24);
        key[5] = (byte) (now >>> 16);
        key[6] = (byte) (now >>> 8);
        key[7] = (byte) now;
        // 分区 ID 4 字节
        key[8] = (byte) (partitionId >>> 24);
        key[9] = (byte) (partitionId >>> 16);
        key[10] = (byte) (partitionId >>> 8);
        key[11] = (byte) partitionId;
        // 序列号 4 字节
        key[12] = (byte) (sequence >>> 24);
        key[13] = (byte) (sequence >>> 16);
        key[14] = (byte) (sequence >>> 8);
        key[15] = (byte) sequence;

        return key;
    }
}
