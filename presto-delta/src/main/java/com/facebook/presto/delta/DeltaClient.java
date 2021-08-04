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
import com.google.inject.Inject;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.types.StructField;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.delta.DeltaErrorCode.DELTA_NO_TABLE;
import static com.facebook.presto.delta.TypeConverter.toPrestoType;
import static java.util.Objects.requireNonNull;

// this is used by Guice to initialize each configured catalog
final class DeltaClient
{
    private final Configuration conf;
    private final DeltaLog deltaTable;  // this should be ConnectorSession-specific beyond the prototype
    private final String schemaName; // this is locked for the while
    private final String location;
    private final String tableName; // this is used verbatim from Config

    @Inject
    public DeltaClient(
            String location,
            String tableName)
    {
        requireNonNull(location, "location is null");
        requireNonNull(tableName, "tableName is null");

        this.conf = new Configuration();
        // REVISIT: the prototype supports one baked-in schema
        this.schemaName = "default";
        this.tableName = tableName;

        this.location = location;
        File dt = Paths.get(location, tableName).toFile();

        if (dt.exists()) {
            this.deltaTable = DeltaLog.forTable(this.conf, dt.getPath());
        }
        else {
            throw new PrestoException(DELTA_NO_TABLE, "No Delta table discovered at location: " + dt.getPath());
        }
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getLocation()
    {
        return location;
    }

    public String getTableName()
    {
        return tableName;
    }

    public List<SchemaTableName> getTables()
    {
        // we only support a single table inside the synthetic schema
        return ImmutableList.of(new SchemaTableName(schemaName, tableName));
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
        // we only support the one baked in schema presently
        if (tableName.getSchemaName() == getSchemaName() && tableName.getTableName() == this.getTableName()) {
            return new DeltaTableHandle(tableName.getSchemaName(),
                    tableName.getTableName(),
                    deltaTable.snapshot().getVersion(),
                    TupleDomain.all());
        }
        else {
            return null;
        }
    }

    public Set<String> getSchemaNames()
    {
        // the prototype is locked to one baked-in schema
        Set<String> lockedSet = new HashSet<>(Arrays.asList(schemaName));
        return lockedSet;
    }
}
