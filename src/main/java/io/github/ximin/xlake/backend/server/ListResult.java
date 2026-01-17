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
package io.github.ximin.xlake.backend.server;

import lombok.Getter;
import org.apache.spark.sql.Row;

import java.util.List;

@Getter
public final class ListResult extends ManagedJobResult {
    private final List<Row> rows;

    public ListResult(ManagedJobResult jobResult, List<Row> rows) {
        this(jobResult.jobId, jobResult.sql, jobResult.status, jobResult.startTime, jobResult.endTime, jobResult.error,rows);
    }

    public ListResult(
            String jobId,
            String sql,
            JobStatus status,
            long startTime,
            long endTime,
            String error,
            List<Row> rows) {
        super(jobId, sql, status, startTime, endTime, error);
        this.rows = rows;
    }
}
