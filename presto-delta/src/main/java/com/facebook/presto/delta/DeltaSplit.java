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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.delta.standalone.FileFormat;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DeltaSplit
        implements ConnectorSplit
{
    private final String pathname;
    private final FileFormat fileFormat;
//    private final DeltaConfig config;

    @JsonCreator
    public DeltaSplit(
            @JsonProperty("path") String path,
            @JsonProperty("fileFormat") FileFormat fileFormat)
//            @JsonProperty("config") DeltaConfig config)
    {
        // HACK to push further
        this.pathname = "/tmp/delta/boston-housing/part-00000-70e5db64-7a0c-4648-b2e8-58c1a8cd35dc-c000.snappy.parquet";
//        this.pathname = requireNonNull(path, "path is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
//        this.config = requireNonNull(config, "config is null");
    }

    @JsonProperty
    public String getPathname()
    {
        return pathname;
    }

    @JsonProperty
    public FileFormat getFileFormat()
    {
        return fileFormat;
    }

//    @JsonProperty
//    public Configuration getHadoopConfig()
//    {
//        return config.getHadoopConf();
//    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        // returning an empty list to indicate no preference
        return new ArrayList<HostAddress>();
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("path", pathname)
                .put("fileFormat", fileFormat)
//                .put("config", config)
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(pathname)
                .addValue(fileFormat)
//                .addValue(config)
                .toString();
    }
}
