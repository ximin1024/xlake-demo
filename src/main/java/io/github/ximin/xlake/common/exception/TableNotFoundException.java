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


public class TableNotFoundException extends CatalogException {

    private static final long serialVersionUID = 1L;

    
    private final String tableIdentifier;

    
    public TableNotFoundException(String message) {
        super(message);
        this.tableIdentifier = null;
    }

    
    public TableNotFoundException(String message, String tableIdentifier) {
        super(message);
        this.tableIdentifier = tableIdentifier;
    }

    
    public TableNotFoundException(String message, Throwable cause) {
        super(message, cause);
        this.tableIdentifier = null;
    }

    
    public String getTableIdentifier() {
        return tableIdentifier;
    }
}
