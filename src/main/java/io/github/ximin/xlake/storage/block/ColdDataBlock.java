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
package io.github.ximin.xlake.storage.block;

import java.util.Map;

public record ColdDataBlock(
        String blockId,
        String tableId,
        DataBlock.Kind kind,
        DataBlock.Format format,
        DataBlock.Format fileFormat,
        DataBlock.Location location,
        DataBlock.KeyRange keyRange,
        long rowCount,
        long sizeBytes,
        long schemaVersion,
        long snapshotId,
        Map<String, String> partitionValues,
        Statistics statistics,
        DataBlock.Visibility visibility,
        long minSequenceNumber,
        long maxSequenceNumber
) implements DataBlock {
    public record Statistics(
            long rowCount,
            long sizeBytes,
            long minSequenceNumber,
            long maxSequenceNumber
    ) {
    }
}
