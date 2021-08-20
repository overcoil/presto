/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.delta;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;

import static java.lang.String.format;

public final class TypeConverter
{
    private TypeConverter()
    {
    }

    public static Type toPrestoType(io.delta.standalone.types.DataType type)
    {
        // REVISIT: there needs to be a better (non-string comparison) way to determine the type
        if (type.getTypeName().equals("integer")) {
            return BigintType.BIGINT;
        }

        if (type.getTypeName().equals("double")) {
            return DoubleType.DOUBLE;
        }

        if (type.getTypeName().equals("float")) {
            return RealType.REAL;
        }

        if (type.getTypeName().equals("string")) {
            return VarcharType.createUnboundedVarcharType();
        }

        if (type.getTypeName().equals("timestamp")) {
            return TimestampType.TIMESTAMP;
        }

        throw new UnsupportedOperationException(format("Cannot convert from Delta type '%s' to Presto type", type.getTypeName()));
    }
}
