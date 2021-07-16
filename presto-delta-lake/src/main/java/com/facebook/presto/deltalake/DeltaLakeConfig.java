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

import com.facebook.airlift.configuration.Config;
import com.facebook.presto.hive.HiveCompressionCodec;

import javax.validation.constraints.NotNull;

import static com.facebook.presto.hive.HiveCompressionCodec.GZIP;
import static com.facebook.presto.deltalake.DeltaLakeFileFormat.PARQUET;

public class DeltaLakeConfig
{
    private DeltaLakeFileFormat fileFormat = PARQUET;
    private HiveCompressionCodec compressionCodec = GZIP;

    @NotNull
    public DeltaLakeFileFormat getFileFormat()
    {
        return DeltaLakeFileFormat.valueOf(fileFormat.name());
    }

    @Config("deltalake.file-format")
    public DeltaLakeConfig setFileFormat(DeltaLakeFileFormat fileFormat)
    {
        this.fileFormat = fileFormat;
        return this;
    }

    @NotNull
    public HiveCompressionCodec getCompressionCodec()
    {
        return compressionCodec;
    }

    @Config("deltalake.compression-codec")
    public DeltaLakeConfig setCompressionCodec(HiveCompressionCodec compressionCodec)
    {
        this.compressionCodec = compressionCodec;
        return this;
    }
}
