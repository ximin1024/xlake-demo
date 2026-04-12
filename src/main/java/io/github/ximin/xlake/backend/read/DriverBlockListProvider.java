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
package io.github.ximin.xlake.backend.read;

import io.github.ximin.xlake.backend.spark.routing.DriverRoutingEndpoint;
import io.github.ximin.xlake.backend.spark.routing.RoutingMessages;
import io.github.ximin.xlake.storage.block.DataBlock;
import org.apache.spark.SparkEnv;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.rpc.RpcTimeout;
import org.apache.spark.util.RpcUtils;

import java.util.List;

public final class DriverBlockListProvider implements DataBlockListProvider {
    private static final long serialVersionUID = 1L;

    @Override
    public List<DataBlock> listBlocks(String basePath, String storeId, String tableIdentifier) {
        SparkEnv env = SparkEnv.get();
        if (env == null || env.rpcEnv() == null) {
            return new LocalTableStoreBlockListProvider().listBlocks(basePath, storeId, tableIdentifier);
        }
        try {
            RpcEndpointRef driverRef = RpcUtils.makeDriverRef(
                    DriverRoutingEndpoint.ENDPOINT_NAME,
                    env.conf(),
                    env.rpcEnv()
            );
            RpcTimeout timeout = RpcUtils.askRpcTimeout(env.conf());
            RoutingMessages.QueryGlobalBlocksMessage query =
                    new RoutingMessages.QueryGlobalBlocksMessage(basePath, storeId, tableIdentifier);
            Object reply = driverRef.askSync(query, timeout,
                    scala.reflect.ClassTag$.MODULE$.apply(Object.class));
            if (reply instanceof RoutingMessages.GlobalBlocksResponse response) {
                return response.blocks();
            }
        } catch (Exception ignored) {
            // Routing plugin not configured — fall back to local block list
        }
        return new LocalTableStoreBlockListProvider().listBlocks(basePath, storeId, tableIdentifier);
    }
}
