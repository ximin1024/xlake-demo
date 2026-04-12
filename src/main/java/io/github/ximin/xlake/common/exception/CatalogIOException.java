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
package io.github.ximin.xlake.common.exception;


public class CatalogIOException extends CatalogException {

    private static final long serialVersionUID = 1L;

    
    private final String storageSystem;

    
    public CatalogIOException(String message) {
        super(message);
        this.storageSystem = null;
    }

    
    public CatalogIOException(String message, String storageSystem) {
        super(message);
        this.storageSystem = storageSystem;
    }

    
    public CatalogIOException(String message, Throwable cause) {
        super(message, cause);
        this.storageSystem = null;
    }

    
    public CatalogIOException(String message, String storageSystem, Throwable cause) {
        super(message, cause);
        this.storageSystem = storageSystem;
    }

    
    public String getStorageSystem() {
        return storageSystem;
    }
}
