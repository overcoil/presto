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
package com.facebook.presto.deltalake;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;

public enum DeltaLakeErrorCode
        implements ErrorCodeSupplier
{
    DELTALAKE_UNKNOWN_TABLE_TYPE(0, EXTERNAL),
    DELTALAKE_INVALID_METADATA(1, EXTERNAL),
    DELTALAKE_TOO_MANY_OPEN_PARTITIONS(2, USER_ERROR),
    DELTALAKE_INVALID_PARTITION_VALUE(3, EXTERNAL),
    DELTALAKE_BAD_DATA(4, EXTERNAL),
    DELTALAKE_MISSING_DATA(5, EXTERNAL),
    DELTALAKE_CANNOT_OPEN_SPLIT(6, EXTERNAL),
    DELTALAKE_WRITER_OPEN_ERROR(7, EXTERNAL), // not needed?
    DELTALAKE_FILESYSTEM_ERROR(8, EXTERNAL),
    DELTALAKE_CURSOR_ERROR(9, EXTERNAL),
    DELTALAKE_WRITE_VALIDATION_FAILED(10, INTERNAL_ERROR),  // not needed?
    DELTALAKE_INVALID_SNAPSHOT_ID(11, USER_ERROR);

    private final ErrorCode errorCode;

    DeltaLakeErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0504_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
