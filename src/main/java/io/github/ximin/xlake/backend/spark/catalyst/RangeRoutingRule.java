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
package io.github.ximin.xlake.backend.spark.catalyst;

import io.github.ximin.xlake.backend.spark.XlakeDataSource;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import scala.Option;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

import java.util.List;

public final class RangeRoutingRule extends Rule<LogicalPlan> {
    private final SparkSession sparkSession;

    public RangeRoutingRule(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        SparkConf conf = sparkSession.sparkContext().conf();
        int shardCount = XlakeDataSource.resolveShardCount(conf);
        if (shardCount <= 1 || !(plan instanceof AppendData appendData)) {
            return plan;
        }
        return shouldRewrite(appendData) ? rewrite(appendData, shardCount) : plan;
    }

    private static boolean shouldRewrite(AppendData appendData) {
        return appendData.table() instanceof DataSourceV2Relation relation
                && XlakeDataSource.isXlakeTable(relation.table())
                && findKeyAttribute(appendData.query()) != null
                && !hasCompatibleRepartition(appendData.query());
    }

    private static LogicalPlan rewrite(AppendData appendData, int shardCount) {
        Attribute keyAttribute = findKeyAttribute(appendData.query());
        if (keyAttribute == null) {
            return appendData;
        }

        RepartitionByExpression repartition = new RepartitionByExpression(
                toSeq(List.of(keyAttribute)),
                appendData.query(),
                Option.apply(shardCount),
                Option.empty()
        );
        return appendData.withNewQuery(repartition);
    }

    static boolean isXlakeRelation(NamedRelation relation) {
        return relation instanceof DataSourceV2Relation dataSourceV2Relation
                && XlakeDataSource.isXlakeTable(dataSourceV2Relation.table());
    }

    static Attribute findKeyAttribute(LogicalPlan plan) {
        for (Attribute attribute : CollectionConverters.asJava(plan.output())) {
            if (XlakeDataSource.KEY_COLUMN.equalsIgnoreCase(attribute.name())) {
                return attribute;
            }
        }
        return null;
    }

    static boolean hasCompatibleRepartition(LogicalPlan plan) {
        if (!(plan instanceof RepartitionByExpression repartition)) {
            return false;
        }
        return CollectionConverters.asJava(repartition.partitionExpressions()).stream()
                .filter(Attribute.class::isInstance)
                .map(Attribute.class::cast)
                .anyMatch(attribute -> XlakeDataSource.KEY_COLUMN.equalsIgnoreCase(attribute.name()));
    }

    private static Seq<Expression> toSeq(List<? extends Expression> expressions) {
        @SuppressWarnings("unchecked")
        Seq<Expression> seq = (Seq<Expression>) (Seq<?>) CollectionConverters.asScala(expressions).toSeq();
        return seq;
    }
}
