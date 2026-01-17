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
package io.github.ximin.xlake.common;

import java.util.Map;
import java.util.TreeMap;

public class RangeManager {
    private final TreeMap<Long, String> rangeMap; // [start, end) -> node

    public RangeManager() {
        this.rangeMap = new TreeMap<>();
    }

    // 添加或合并区间
    public void addRange(long start, long end, String node) {
        rangeMap.put(start, node);

        // 尝试合并相邻区间
        Map.Entry<Long, String> prev = rangeMap.lowerEntry(start);
        if (prev != null && prev.getValue().equals(node)) {
            rangeMap.remove(start);
            start = prev.getKey();
        }

        Map.Entry<Long, String> next = rangeMap.higherEntry(end);
        if (next != null && next.getValue().equals(node)) {
            rangeMap.remove(next.getKey());
            end = next.getKey();
        }

        rangeMap.put(start, node);
    }

    public String findNode(long bucket) {
        Map.Entry<Long, String> entry = rangeMap.floorEntry(bucket);
        return entry != null ? entry.getValue() : null;
    }
}
