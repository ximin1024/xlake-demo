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
package io.github.ximin.xlake.common.utls;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileUtils {

    public static List<Path> findMatchingPaths(String basePath) throws IOException {
        List<Path> matchingPaths = new ArrayList<>();
        Path baseDir = Paths.get(basePath).getParent(); // 获取同级目录

        if (baseDir == null) {
            throw new IllegalArgumentException("Invalid base path: " + basePath);
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(baseDir)) {
            for (Path path : stream) {
                if (path.toString().startsWith(basePath)) {
                    matchingPaths.add(path);
                }
            }
        }

        return matchingPaths;
    }
}
