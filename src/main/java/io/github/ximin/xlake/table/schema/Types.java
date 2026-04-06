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
package io.github.ximin.xlake.table.schema;

public final class Types {

    private Types() {
    }

    public static Int64Type int64() {
        return new Int64Type();
    }

    public static Int64Type int64(boolean nullable) {
        return new Int64Type(nullable);
    }

    public static Int32Type int32() {
        return new Int32Type();
    }

    public static Int32Type int32(boolean nullable) {
        return new Int32Type(nullable);
    }

    public static Int8Type int8() {
        return new Int8Type();
    }

    public static Int8Type int8(boolean nullable) {
        return new Int8Type(nullable);
    }

    public static StringType string() {
        return new StringType();
    }

    public static StringType string(boolean nullable) {
        return new StringType(nullable);
    }

    public static TimestampType timestamp() {
        return new TimestampType();
    }

    public static TimestampType timestamp(boolean nullable) {
        return new TimestampType(nullable);
    }

    public static DateType date() {
        return new DateType();
    }

    public static DateType date(boolean nullable) {
        return new DateType(nullable);
    }

    public static BooleanType bool() {
        return new BooleanType();
    }

    public static BooleanType bool(boolean nullable) {
        return new BooleanType(nullable);
    }

    public static Float32Type float32() {
        return new Float32Type();
    }

    public static Float32Type float32(boolean nullable) {
        return new Float32Type(nullable);
    }

    public static Float64Type float64() {
        return new Float64Type();
    }

    public static Float64Type float64(boolean nullable) {
        return new Float64Type(nullable);
    }

    public static DecimalType decimal(int precision, int scale) {
        return new DecimalType(precision, scale);
    }

    public static DecimalType decimal(int precision, int scale, boolean nullable) {
        return new DecimalType(precision, scale, nullable, null);
    }

    public static BinaryType binary() {
        return new BinaryType();
    }

    public static BinaryType binary(boolean nullable) {
        return new BinaryType(nullable);
    }
}
