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
package io.github.ximin.xlake.backend.spark;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;
import scala.reflect.ClassTag;

import java.util.List;
import java.util.Map;

public final class PreferredLocationRDD<T> extends RDD<T> {
    private final RDD<T> parent;
    private final Map<Integer, List<String>> preferredLocationsByPartition;

    public PreferredLocationRDD(RDD<T> parent,
                                Map<Integer, List<String>> preferredLocationsByPartition,
                                ClassTag<T> evidence) {
        super(parent, evidence);
        this.parent = parent;
        this.preferredLocationsByPartition = preferredLocationsByPartition;
    }

    @Override
    public scala.collection.Iterator<T> compute(Partition split, TaskContext context) {
        return parent.iterator(split, context);
    }

    @Override
    public Partition[] getPartitions() {
        return parent.partitions();
    }

    @Override
    public Seq<String> getPreferredLocations(Partition split) {
        List<String> locs = preferredLocationsByPartition.get(split.index());
        if (locs == null || locs.isEmpty()) {
            return parent.preferredLocations(split);
        }
        return JavaConverters.asScalaIteratorConverter(locs.iterator()).asScala().toSeq();
    }
}
