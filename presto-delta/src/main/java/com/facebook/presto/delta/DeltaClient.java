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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.types.StructField;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.inject.Inject;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.delta.DeltaErrorCode.DELTA_NO_TABLE;
import static com.facebook.presto.delta.TypeConverter.toPrestoType;

public class DeltaClient
{
    private DeltaConfig config;
    private Configuration hadoopConf;
    private DeltaLog deltaTable;
    private String pathName = "/path/to/deltalake/table-dir";

    @Inject
    public DeltaClient(DeltaConfig config)
    {
        this.config = config;
        this.pathName = config.getPathname();

        this.hadoopConf = new org.apache.hadoop.conf.Configuration();

        File file = Paths.get(this.pathName).toFile();
        if (file.exists()) {
            this.deltaTable = DeltaLog.forTable(this.hadoopConf, this.pathName);
        }
        else {
            throw new PrestoException(DELTA_NO_TABLE, "No Delta table discovered at location: " + file.getPath());
        }
    }

    // Alternate c-tor for manual recreation of the client
    public DeltaClient(String location, String tableName)
    {
        this.config = null;
        this.hadoopConf = new org.apache.hadoop.conf.Configuration();

        File file = Paths.get(location, tableName).toFile();

        if (file.exists()) {
            this.pathName = file.toString();
            this.deltaTable = DeltaLog.forTable(this.hadoopConf, this.pathName);
        }
        else {
            throw new PrestoException(DELTA_NO_TABLE, "No Delta table discovered at location: " + file.getPath());
        }
    }

    @JsonCreator
    public DeltaClient(@JsonProperty("pathName") String pathName)
    {
        this.config = null;
        this.hadoopConf = new Configuration();

        this.pathName = pathName;
        this.deltaTable = DeltaLog.forTable(this.hadoopConf, this.pathName);
    }

    @JsonProperty
    public String getPathName()
    {
        return pathName;
    }

    public Set<String> getSchemaNames()
    {
        Set<String> lockedSet = new HashSet<>(Arrays.asList(config.getSchemaName()));
        return lockedSet;
    }

    public List<SchemaTableName> getTables()
    {
        // we only support a single table inside the synthetic schema
        return ImmutableList.of(new SchemaTableName(config.getSchemaName(), config.getTableName()));
    }

    public List<ColumnMetadata> getColumns(DeltaTableHandle tableHandle)
    {
        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.<ColumnMetadata>builder();

        // REVISIT: This handles only primitive types in Delta's schema for the while.
        // As well, toPrestoType only supports a limited set of primitive types at this time.
        for (StructField column : deltaTable.snapshot().getMetadata().getSchema().getFields()) {
            ColumnMetadata item = ColumnMetadata.builder()
                    .setName(column.getName())
                    .setType(toPrestoType(column.getDataType()))
                    .setNullable(column.isNullable())
                    .build();
            columnMetadataBuilder.add(item);
        }

        return columnMetadataBuilder.build();
    }

    public DeltaTableHandle getTableHandle(SchemaTableName tableName)
    {
        return new DeltaTableHandle(tableName.getSchemaName(),
                tableName.getTableName(),
                pathName,
                TupleDomain.all());
    }

    public Configuration getHadoopConfig()
    {
        return hadoopConf;
    }

    public Snapshot getSnapshot()
    {
        return deltaTable.snapshot();
    }
}
