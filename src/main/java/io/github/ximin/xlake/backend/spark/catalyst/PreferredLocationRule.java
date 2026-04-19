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
import io.github.ximin.xlake.backend.spark.XlakeWriteTopology;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.AppendDataExec;

import java.util.List;
import java.util.Map;

public final class PreferredLocationRule extends Rule<SparkPlan> {
    private final SparkSession sparkSession;

    public PreferredLocationRule(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public SparkPlan apply(SparkPlan plan) {
        if (!(plan instanceof AppendDataExec appendDataExec)) {
            return plan;
        }
        if (!XlakeDataSource.isXlakeWrite(appendDataExec.write())) {
            return plan;
        }

        int shardCount = XlakeDataSource.resolveShardCount(sparkSession.sparkContext().conf());
        if (shardCount <= 1) {
            return plan;
        }

        XlakeWriteTopology topology = XlakeWriteTopology.inspect(sparkSession.sparkContext());
        if (!topology.supportsPreferredLocations()) {
            return plan;
        }

        if (appendDataExec.query().outputPartitioning().numPartitions() != shardCount) {
            return plan;
        }

        String driverHost = sparkSession.sparkContext().conf().get("spark.driver.host", "");
        Map<Integer, List<String>> preferredLocations = topology.preferredLocationsByShard(shardCount, driverHost);
        if (preferredLocations.isEmpty()) {
            return plan;
        }

        SparkPlan newChild = new PreferredLocationExec(appendDataExec.child(), preferredLocations);
        return appendDataExec.withNewChildInternal(newChild);
    }
}
