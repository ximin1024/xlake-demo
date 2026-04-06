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
package io.github.ximin.xlake.table.op;

import io.github.ximin.xlake.backend.query.Expression;
import java.io.Serializable;
import java.util.List;

public interface Find extends Read {

    Expression filter();

    List<String> projection();

    long snapshotId();

    boolean caseSensitive();

    long limit();

    default ReadContext asContext() {
        return new DefaultReadContext(filter(), snapshotId(), caseSensitive(), limit());
    }

    interface ReadContext {
        Expression filter();
        long snapshotId();
        boolean caseSensitive();
        long limit();
    }

    record DefaultReadContext(
            Expression filter,
            long snapshotId,
            boolean caseSensitive,
            long limit
    ) implements ReadContext {
    }

    record FindResult(
            boolean found,
            Object data
    ) implements Serializable {
    }
}
