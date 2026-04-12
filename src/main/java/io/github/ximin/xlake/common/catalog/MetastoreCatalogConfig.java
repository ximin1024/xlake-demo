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
package io.github.ximin.xlake.common.catalog;

import io.github.ximin.xlake.metastore.Metastore;

import java.util.Objects;

public class MetastoreCatalogConfig implements CatalogConfig {
    private final Metastore metastore;


    private final String basePath;


    private final String catalogName;

    private MetastoreCatalogConfig(Builder builder) {
        this.metastore = Objects.requireNonNull(builder.metastore,
                "metastore is required for MetastoreCatalogConfig");
        this.basePath = Objects.requireNonNull(builder.basePath,
                "basePath is required for MetastoreCatalogConfig");
        this.catalogName = builder.catalogName != null ? builder.catalogName : "default";
    }

    public Metastore metastore() {
        return metastore;
    }

    public String basePath() {
        return basePath;
    }

    public String catalogName() {
        return catalogName;
    }

    public static Builder builder() {
        return new Builder();
    }


    public static class Builder {
        private Metastore metastore;
        private String basePath;
        private String catalogName;

        public Builder metastore(Metastore metastore) {
            this.metastore = metastore;
            return this;
        }

        public Builder basePath(String basePath) {
            this.basePath = basePath;
            return this;
        }

        public Builder catalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }


        public MetastoreCatalogConfig build() {
            // [SEVERE-003修复] 前置校验：确保必要参数已设置
            if (this.metastore == null) {
                throw new IllegalStateException(
                        "metastore is required and cannot be null. " +
                                "Please call .metastore(metastoreInstance) before building."
                );
            }

            if (this.basePath == null || this.basePath.isBlank()) {
                throw new IllegalStateException(
                        "basePath is required and cannot be null or blank. " +
                                "Please call .basePath('/path/to/storage') before building."
                );
            }

            return new MetastoreCatalogConfig(this);
        }
    }
}
