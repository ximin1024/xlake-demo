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
package io.github.ximin.xlake.storage.table.record;

import io.github.ximin.xlake.table.record.RecordView;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ProjectedRecordView implements RecordView {
    private final RecordView delegate;
    private final Set<String> projectedFields;

    public ProjectedRecordView(RecordView delegate, List<String> projectedFields) {
        this.delegate = delegate;
        this.projectedFields = new LinkedHashSet<>(projectedFields);
    }

    @Override
    public byte[] key() {
        return delegate.key();
    }

    @Override
    public byte[] value() {
        return delegate.value();
    }

    @Override
    public boolean hasField(String fieldName) {
        return projectedFields.contains(fieldName) && delegate.hasField(fieldName);
    }

    @Override
    public Object field(String fieldName) {
        if (!projectedFields.contains(fieldName)) {
            return null;
        }
        return delegate.field(fieldName);
    }
}
