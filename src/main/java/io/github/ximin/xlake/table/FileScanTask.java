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
package io.github.ximin.xlake.table;

public record FileScanTask(
        String filePath,
        long fileSizeBytes,
        String partitionPath,
        long startOffset,
        long length
) {
    public static FileScanTask of(String filePath, long fileSizeBytes) {
        return new FileScanTask(filePath, fileSizeBytes, null, 0, fileSizeBytes);
    }

    public static FileScanTask of(String filePath, long fileSizeBytes, String partitionPath) {
        return new FileScanTask(filePath, fileSizeBytes, partitionPath, 0, fileSizeBytes);
    }

    public boolean isFullFile() {
        return startOffset == 0 && length == fileSizeBytes;
    }
}
