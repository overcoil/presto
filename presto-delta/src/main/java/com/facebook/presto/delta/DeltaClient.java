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
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.delta.DeltaErrorCode.DELTA_NO_TABLE;
import static com.facebook.presto.delta.TypeConverter.toPrestoType;

public class DeltaClient
{
    private Configuration hadoopConf;

    private String effectiveDeltaTableName;
    private DeltaLog deltaTable;

    private Map<String, String> availableTables;

    // only the following fields are persistent
    private String schemaName = "default";
    private String location = "/tmp/delta";
    private String tableExpr = "*";

    @Inject
    public DeltaClient(DeltaConfig config)
    {
        // do this early
        this.hadoopConf = new org.apache.hadoop.conf.Configuration();

        // defer DeltaLog setup and table scan
        this.effectiveDeltaTableName = null;
        this.deltaTable = null;

        this.availableTables = null;

        this.schemaName = config.getSchemaName();
        this.location = config.getLocation();
        this.tableExpr = config.getTableName();
    }

    @JsonCreator
    public DeltaClient(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("location") String location,
            @JsonProperty("tableExpr") String tableExpr)
    {
        this.hadoopConf = new org.apache.hadoop.conf.Configuration();

        // defer DeltaLog setup and table scan
        this.effectiveDeltaTableName = null;
        this.deltaTable = null;

        this.availableTables = null;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public String getTableExpr()
    {
        return tableExpr;
    }

    public Set<String> getSchemaNames()
    {
        Set<String> lockedSet = new HashSet<>(Arrays.asList(getSchemaName()));
        return lockedSet;
    }

    private void initTables()
    {
        // do a scan if we haven't already
        if (availableTables == null) {
            Path catalogueRoot = Paths.get(location);

            // implicit wildcard if no table expression is specified
            if (tableExpr.isEmpty()) {
                tableExpr = "*";
            }

            this.availableTables = new HashMap<String, String>();

            try (DirectoryStream<Path> paths = Files.newDirectoryStream(catalogueRoot, tableExpr)) {
                for (Path path : paths) {
                    // the discriminator is the presence of a _delta_lake directory
                    if (Paths.get(path.toString(), "_delta_log").toFile().exists()) {
                        String s = path.getFileName().toString().toLowerCase();
                        availableTables.put(path.getFileName().toString().toLowerCase(), path.toString());
                    }
                }
            }
            catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public List<SchemaTableName> getTables()
    {
        initTables();

        ImmutableList.Builder<SchemaTableName> tablesBuilder = ImmutableList.builder();
        availableTables.forEach((k, v) -> tablesBuilder.add(new SchemaTableName(getSchemaName(), k)));
        return tablesBuilder.build();
    }

    private void initDSR(String tableName)
    {
        // open the DeltaLog if havent' previously *or* re-open if this is a different table than what was opened previously
        if (deltaTable == null || !effectiveDeltaTableName.equals(tableName)) {
            // we do not rely initTables() though maybe we can...
            File file = Paths.get(location, tableName).toFile();
            if (file.exists()) {
                effectiveDeltaTableName = tableName;
                deltaTable = DeltaLog.forTable(hadoopConf, file.toString());
            }
            else {
                throw new PrestoException(DELTA_NO_TABLE, "No such Delta table " + tableName);
            }
        }
    }

    public List<ColumnMetadata> getColumns(DeltaTableHandle tableHandle)
    {
        initDSR(tableHandle.getTableName());

        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.<ColumnMetadata>builder();

        // NB: toPrestoType only supports a limited set of primitive types at this time.
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
                getPathForTable(tableName.getTableName()),
                TupleDomain.all());
    }

    public Configuration getHadoopConfig()
    {
        return hadoopConf;
    }

    public String getPathForTable(String tableName)
    {
        initTables();

        return availableTables.get(tableName);
    }

    public Snapshot getSnapshotForTable(String tableName)
    {
        initDSR(tableName);

        return deltaTable.snapshot();
    }
}
