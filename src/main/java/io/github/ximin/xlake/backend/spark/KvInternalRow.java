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

import io.github.ximin.xlake.table.schema.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

public class KvInternalRow extends InternalRow {
    private final byte[] key;
    private final byte[] value;
    private final Schema schema;

    public KvInternalRow(byte[] key, byte[] value, Schema schema) {
        this.key = key;
        this.value = value;
        this.schema = schema;
    }

    @Override
    public int numFields() {
        return 0;
    }

    @Override
    public void setNullAt(int i) {

    }

    @Override
    public void update(int i, Object value) {

    }

    @Override
    public InternalRow copy() {
        return null;
    }

    @Override
    public boolean isNullAt(int i) {
        return false;
    }

    @Override
    public boolean getBoolean(int i) {
        return false;
    }

    @Override
    public byte getByte(int i) {
        return 0;
    }

    @Override
    public short getShort(int i) {
        return 0;
    }

    @Override
    public int getInt(int i) {
        return 0;
    }

    @Override
    public long getLong(int i) {
        return 0;
    }

    @Override
    public float getFloat(int i) {
        return 0;
    }

    @Override
    public double getDouble(int i) {
        return 0;
    }

    @Override
    public Decimal getDecimal(int i, int i1, int i2) {
        return null;
    }

    @Override
    public UTF8String getUTF8String(int i) {
        return null;
    }

    @Override
    public byte[] getBinary(int i) {
        return new byte[0];
    }

    @Override
    public CalendarInterval getInterval(int i) {
        return null;
    }

    @Override
    public VariantVal getVariant(int i) {
        return null;
    }

    @Override
    public InternalRow getStruct(int i, int i1) {
        return null;
    }

    @Override
    public ArrayData getArray(int i) {
        return null;
    }

    @Override
    public MapData getMap(int i) {
        return null;
    }

    @Override
    public Object get(int i, DataType dataType) {
        return null;
    }
}
