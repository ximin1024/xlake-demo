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

import io.github.ximin.xlake.backend.spark.routing.DriverRoutingPlugin;
import io.github.ximin.xlake.backend.spark.routing.ExecutorRoutingPlugin;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

import java.util.Objects;

public final class XlakeSparkPlugin implements SparkPlugin {
    private final DriverPlugin driverPlugin;
    private final ExecutorPlugin executorPlugin;

    public XlakeSparkPlugin() {
        this(new DriverRoutingPlugin(), new ExecutorRoutingPlugin());
    }

    XlakeSparkPlugin(DriverPlugin driverPlugin, ExecutorPlugin executorPlugin) {
        this.driverPlugin = Objects.requireNonNull(driverPlugin, "driverPlugin must not be null");
        this.executorPlugin = Objects.requireNonNull(executorPlugin, "executorPlugin must not be null");
    }

    @Override
    public DriverPlugin driverPlugin() {
        return driverPlugin;
    }

    @Override
    public ExecutorPlugin executorPlugin() {
        return executorPlugin;
    }
}
