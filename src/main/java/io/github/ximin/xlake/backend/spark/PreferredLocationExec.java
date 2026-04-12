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

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.execution.SparkPlan;
import scala.reflect.ClassTag$;

import java.util.List;
import java.util.Map;

public final class PreferredLocationExec extends SparkPlan {
    private final SparkPlan child;
    private final Map<Integer, List<String>> preferredLocations;

    public PreferredLocationExec(SparkPlan child, Map<Integer, List<String>> preferredLocations) {
        this.child = child;
        this.preferredLocations = preferredLocations;
    }

    @Override
    public scala.collection.immutable.Seq<Attribute> output() {
        return child.output();
    }

    @Override
    public org.apache.spark.sql.catalyst.plans.physical.Partitioning outputPartitioning() {
        return child.outputPartitioning();
    }

    @Override
    public RDD<InternalRow> doExecute() {
        return new PreferredLocationRDD<>(
                child.execute(),
                preferredLocations,
                ClassTag$.MODULE$.apply(InternalRow.class)
        );
    }

    public SparkPlan withNewChildInternal(SparkPlan newChild) {
        return new PreferredLocationExec(newChild, preferredLocations);
    }

    @Override
    public scala.collection.immutable.Seq<SparkPlan> children() {
        return scala.collection.immutable.List.from(scala.jdk.javaapi.CollectionConverters.asScala(List.of(child)));
    }

    @Override
    public SparkPlan withNewChildrenInternal(scala.collection.immutable.IndexedSeq<SparkPlan> newChildren) {
        SparkPlan only = newChildren.apply(0);
        return withNewChildInternal(only);
    }

    @Override
    public int productArity() {
        return 1;
    }

    @Override
    public Object productElement(int n) {
        if (n == 0) {
            return child;
        }
        throw new IndexOutOfBoundsException(Integer.toString(n));
    }

    @Override
    public boolean canEqual(Object that) {
        return that instanceof PreferredLocationExec;
    }

    @Override
    public String productPrefix() {
        return "XlakePreferredLocationExec";
    }
}
