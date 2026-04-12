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
package io.github.ximin.xlake.backend.read;

import io.github.ximin.xlake.storage.block.DataBlock;
import org.apache.spark.sql.sources.*;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.charset.StandardCharsets;
import java.util.*;

public final class ReadPruning {
    private ReadPruning() {
    }

    
    public record KeyPruningSpec(
            boolean fullScan,
            boolean noMatch,
            DataBlock.KeyRange keyRange,
            List<byte[]> pointKeys
    ) {
        public static KeyPruningSpec fullScanSpec() {
            return new KeyPruningSpec(true, false, null, List.of());
        }

        public static KeyPruningSpec noMatchSpec() {
            return new KeyPruningSpec(false, true, null, List.of());
        }

        public static KeyPruningSpec of(DataBlock.KeyRange keyRange, List<byte[]> points) {
            return new KeyPruningSpec(false, false, keyRange, points == null ? List.of() : List.copyOf(points));
        }
    }

    
    public static KeyPruningSpec extractKeyPruningSpec(Filter[] filters, String keyColumn) {
        if (filters == null || filters.length == 0) {
            return KeyPruningSpec.of(null, List.of());
        }
        Objects.requireNonNull(keyColumn, "keyColumn");
        ArrayList<Filter> flat = new ArrayList<>();
        for (Filter f : filters) {
            flattenAnd(f, flat);
        }

        // 范围约束（交集）
        byte[] lower = null;
        boolean lowerInclusive = true;
        byte[] upper = null;
        boolean upperInclusive = true;

        // 点集合（交集语义：多次等值/IN 取交）
        Set<ByteArrayWrapper> points = null;

        for (Filter f : flat) {
            if (f == null) {
                continue;
            }
            if (f instanceof Or || f instanceof Not) {
                return KeyPruningSpec.fullScanSpec();
            }

            if (f instanceof EqualTo eq) {
                if (!keyColumn.equalsIgnoreCase(eq.attribute())) {
                    continue;
                }
                byte[] key = literalToBytes(eq.value());
                if (key == null) {
                    return KeyPruningSpec.fullScanSpec();
                }
                points = intersectPoints(points, Set.of(new ByteArrayWrapper(key)));
                // 等值也可以贡献一个精确 range
                lower = key;
                lowerInclusive = true;
                upper = key;
                upperInclusive = true;
                continue;
            }

            if (f instanceof In in) {
                if (!keyColumn.equalsIgnoreCase(in.attribute())) {
                    continue;
                }
                Object[] values = in.values();
                if (values == null || values.length == 0) {
                    return KeyPruningSpec.noMatchSpec();
                }
                LinkedHashSet<ByteArrayWrapper> cur = new LinkedHashSet<>();
                for (Object v : values) {
                    byte[] key = literalToBytes(v);
                    if (key == null) {
                        return KeyPruningSpec.fullScanSpec();
                    }
                    cur.add(new ByteArrayWrapper(key));
                }
                points = intersectPoints(points, cur);
                continue;
            }

            if (f instanceof GreaterThanOrEqual ge) {
                if (!keyColumn.equalsIgnoreCase(ge.attribute())) {
                    continue;
                }
                byte[] bound = literalToBytes(ge.value());
                if (bound == null) {
                    return KeyPruningSpec.fullScanSpec();
                }
                if (lower == null || ByteArrays.compareUnsigned(bound, lower) > 0
                        || ByteArrays.compareUnsigned(bound, lower) == 0 && !lowerInclusive) {
                    lower = bound;
                    lowerInclusive = true;
                }
                continue;
            }

            if (f instanceof GreaterThan gt) {
                if (!keyColumn.equalsIgnoreCase(gt.attribute())) {
                    continue;
                }
                byte[] bound = literalToBytes(gt.value());
                if (bound == null) {
                    return KeyPruningSpec.fullScanSpec();
                }
                if (lower == null || ByteArrays.compareUnsigned(bound, lower) > 0
                        || ByteArrays.compareUnsigned(bound, lower) == 0 && lowerInclusive) {
                    lower = bound;
                    lowerInclusive = false;
                }
                continue;
            }

            if (f instanceof LessThanOrEqual le) {
                if (!keyColumn.equalsIgnoreCase(le.attribute())) {
                    continue;
                }
                byte[] bound = literalToBytes(le.value());
                if (bound == null) {
                    return KeyPruningSpec.fullScanSpec();
                }
                if (upper == null || ByteArrays.compareUnsigned(bound, upper) < 0
                        || ByteArrays.compareUnsigned(bound, upper) == 0 && !upperInclusive) {
                    upper = bound;
                    upperInclusive = true;
                }
                continue;
            }

            if (f instanceof LessThan lt) {
                if (!keyColumn.equalsIgnoreCase(lt.attribute())) {
                    continue;
                }
                byte[] bound = literalToBytes(lt.value());
                if (bound == null) {
                    return KeyPruningSpec.fullScanSpec();
                }
                if (upper == null || ByteArrays.compareUnsigned(bound, upper) < 0
                        || ByteArrays.compareUnsigned(bound, upper) == 0 && upperInclusive) {
                    upper = bound;
                    upperInclusive = false;
                }
                continue;
            }

            // 其它 filter：不影响 block 裁剪（交给 Spark 上层执行），但也不算“无法解析”
        }

        // range 合法性检查
        if (lower != null && upper != null) {
            int cmp = ByteArrays.compareUnsigned(lower, upper);
            if (cmp > 0) {
                return KeyPruningSpec.noMatchSpec();
            }
            if (cmp == 0 && (!lowerInclusive || !upperInclusive)) {
                return KeyPruningSpec.noMatchSpec();
            }
        }

        List<byte[]> pointKeys = List.of();
        if (points != null) {
            // 用 range 过滤 points，避免无谓 block 选择
            ArrayList<byte[]> filtered = new ArrayList<>(points.size());
            for (ByteArrayWrapper w : points) {
                byte[] k = w.bytes();
                if (lower != null) {
                    int c = ByteArrays.compareUnsigned(k, lower);
                    if (c < 0 || c == 0 && !lowerInclusive) {
                        continue;
                    }
                }
                if (upper != null) {
                    int c = ByteArrays.compareUnsigned(k, upper);
                    if (c > 0 || c == 0 && !upperInclusive) {
                        continue;
                    }
                }
                filtered.add(k);
            }
            if (filtered.isEmpty()) {
                return KeyPruningSpec.noMatchSpec();
            }
            pointKeys = List.copyOf(filtered);
        }

        DataBlock.KeyRange keyRange = null;
        if (lower != null || upper != null) {
            keyRange = new DataBlock.KeyRange(lower == null ? new byte[0] : lower, lowerInclusive,
                    upper == null ? new byte[0] : upper, upperInclusive);
        }
        return KeyPruningSpec.of(keyRange, pointKeys);
    }

    public static List<DataBlock> pruneBlocks(List<DataBlock> blocks, KeyPruningSpec spec) {
        if (blocks == null || blocks.isEmpty()) {
            return List.of();
        }
        if (spec == null || spec.fullScan()) {
            return blocks;
        }
        if (spec.noMatch()) {
            return List.of();
        }

        List<byte[]> points = spec.pointKeys();
        if (points != null && !points.isEmpty()) {
            ArrayList<DataBlock> hit = new ArrayList<>();
            for (DataBlock block : blocks) {
                if (block != null && block.kind() == DataBlock.Kind.MUTABLE_HOT) {
                    hit.add(block);
                    continue;
                }
                DataBlock.KeyRange range = block == null ? null : block.keyRange();
                if (range == null) {
                    // 无范围信息：保守起见不能裁剪
                    hit.add(block);
                    continue;
                }
                for (byte[] k : points) {
                    if (range.contains(k)) {
                        hit.add(block);
                        break;
                    }
                }
            }
            return hit;
        }

        DataBlock.KeyRange queryRange = spec.keyRange();
        if (queryRange == null) {
            return blocks;
        }
        return blocks.stream()
                .filter(Objects::nonNull)
                .filter(block -> block.kind() == DataBlock.Kind.MUTABLE_HOT || block.keyRange() == null || block.keyRange().overlaps(queryRange))
                .toList();
    }

    public static String preferredHost(DataBlock block) {
        if (block == null || block.location() == null) {
            return null;
        }
        String host = block.location().storageName();
        if (host == null) {
            return null;
        }
        host = host.trim();
        if (host.isEmpty() || "local".equalsIgnoreCase(host)) {
            return null;
        }
        return host;
    }

    private static void flattenAnd(Filter filter, List<Filter> out) {
        if (filter == null) {
            return;
        }
        if (filter instanceof And and) {
            flattenAnd(and.left(), out);
            flattenAnd(and.right(), out);
            return;
        }
        out.add(filter);
    }

    private static Set<ByteArrayWrapper> intersectPoints(Set<ByteArrayWrapper> prev, Set<ByteArrayWrapper> cur) {
        if (prev == null) {
            return new LinkedHashSet<>(cur);
        }
        LinkedHashSet<ByteArrayWrapper> next = new LinkedHashSet<>(prev);
        next.retainAll(cur);
        return next;
    }

    private static byte[] literalToBytes(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof UTF8String utf8) {
            return utf8.getBytes();
        }
        if (value instanceof CharSequence seq) {
            return seq.toString().getBytes(StandardCharsets.UTF_8);
        }
        return null;
    }

    private record ByteArrayWrapper(byte[] bytes) {
        private ByteArrayWrapper {
            Objects.requireNonNull(bytes, "bytes");
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ByteArrayWrapper other)) {
                return false;
            }
            return Arrays.equals(bytes, other.bytes);
        }
        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    private static final class ByteArrays {
        private ByteArrays() {
        }

        static int compareUnsigned(byte[] left, byte[] right) {
            int min = Math.min(left.length, right.length);
            for (int i = 0; i < min; i++) {
                int cmp = Byte.compareUnsigned(left[i], right[i]);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return Integer.compare(left.length, right.length);
        }
    }
}
