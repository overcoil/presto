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
package com.facebook.presto.delta; // Iceberg parallel is

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class DeltaTables
{
    private final DeltaLog deltaTable;

    private final Map<SchemaTableName, DeltaClient> tableDataLocations;
    private final Map<SchemaTableName, DeltaTableHandle> tables;
    private final Map<SchemaTableName, List<ColumnMetadata>> tableColumns;

    // TODO: This current implementation only support a single table at the specified location but is
    //  generalizable to support multiple tables.
    @Inject
    public DeltaTables(DeltaConfig config)
    {
        ImmutableMap.Builder<SchemaTableName, DeltaClient> dataLocationBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<SchemaTableName, DeltaTableHandle> tablesBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumnsBuilder = ImmutableMap.builder();

        // these are verbatim from the .properties file; this will be rectified into a canonical location:table pair
        String configLocation = config.getDeltaLocation();
        String configTable = config.getDeltaTable();
        String schemaName;

        // TODO: the following only validates location+table as a single table;
        if (configTable.isEmpty()) {
            File dir = Paths.get(configLocation).toAbsolutePath().toFile();

            if (dir.exists()) {
                // since the Delta table was specified directly via location, the schema will be a stock "default"
                schemaName = "default";

                // if the directory exists, validate there's at least a _delta_log subdirectory inside
                File dl = Paths.get(configLocation, "_delta_log").toFile();

                if (dl.exists()) {
                    // strip off the last name from location to use for the table name
                    // TODO: table needs to supports a regex here in the future and build out the Maps above
                    configTable = dir.getName();
                    // then rewrite location to be its parent
                    configLocation = dir.getParentFile().toString();
                }
            }
        }
        else {
            // check the combined path is either one Delta table or a directory containing (directly) one or more Delta tables
            File dir = Paths.get(configLocation, configTable).toAbsolutePath().toFile();

            if (dir.exists()) {
                // extract "the last name in the pathname's name sequence" to use as the VisibleSchemaName;
                // in case the location is at the root, use a default
                schemaName = dir.getName();
                if (schemaName.isEmpty()) {
                    schemaName = "default";
                }

                // check for a contained _delta_log directory
                File dl = Paths.get(configLocation, configTable, "_delta_log").toFile();

                // TODO
                if (!dl.exists()) {
                    // error out
                }

                // NOP since location and table are already in useable form for a single table
            }
            else {
                // error out
            }
        }

        String path = Paths.get(configLocation, configTable).toString();

        deltaTable = DeltaLog.forTable(new Configuration(), path);
        Snapshot snapShot = deltaTable.snapshot();
        Metadata metaData = snapShot.getMetadata();
        StructType columns = metaData.getSchema();

        schemaName = "default";
        SchemaTableName table = new SchemaTableName(schemaName, configTable);
        DeltaClient dataLocation = new DeltaClient(configLocation, Optional.of(configTable));

        DeltaTableHandle tableHandle = new DeltaTableHandle(schemaName, configTable, Long.valueOf(snapShot.getVersion()), TupleDomain.all());

        tablesBuilder.put(table, tableHandle);

        // TODO: beware toPrestoType() is 1) grossly inefficient; & 2) support only 3 types presently
        List<ColumnMetadata> cols = new ArrayList<>();
        for (StructField field : columns.getFields()) {
            cols.add(ColumnMetadata.builder()
                    .setName(field.getName())
                    .setType(TypeConverter.toPrestoType(field.getDataType()))
                    .setNullable(field.isNullable())
                    .build());
        }
        tableColumnsBuilder.put(table, cols);
        dataLocationBuilder.put(table, dataLocation);

        tables = tablesBuilder.build();
        tableColumns = tableColumnsBuilder.build();
        tableDataLocations = dataLocationBuilder.build();
    }

    public DeltaTableHandle getTable(SchemaTableName tableName)
    {
        return tables.get(tableName);
    }

    public List<SchemaTableName> getTables()
    {
        return ImmutableList.copyOf(tables.keySet());
    }

    public List<ColumnMetadata> getColumns(DeltaTableHandle tableHandle)
    {
        checkArgument(tableColumns.containsKey(tableHandle.getSchemaTableName()), "Table %s not registered", tableHandle.getSchemaTableName());
        return tableColumns.get(tableHandle.getSchemaTableName());
    }

    // relocated from LocalFileTables::HttpRequestLogTable and turned into an instance method
    public static List<ColumnMetadata> getColumns()
    {
        return new ArrayList<ColumnMetadata>();
    }

    // relocated from LocalFileTables::HttpRequestLogTable and turned into an instance method
    public static SchemaTableName getSchemaTableName(String schemaName)
    {
        return new SchemaTableName(schemaName, "schema");
    }

    /*
    public static class HttpRequestLogTable
    {
        private static final List<ColumnMetadata> COLUMNS = ImmutableList.of(
                SERVER_ADDRESS_COLUMN,
                new ColumnMetadata("timestamp", TIMESTAMP),
                new ColumnMetadata("client_address", createUnboundedVarcharType()),
                new ColumnMetadata("method", createUnboundedVarcharType()),
                new ColumnMetadata("request_uri", createUnboundedVarcharType()),
                new ColumnMetadata("user", createUnboundedVarcharType()),
                new ColumnMetadata("agent", createUnboundedVarcharType()),
                new ColumnMetadata("response_code", BIGINT),
                new ColumnMetadata("request_size", BIGINT),
                new ColumnMetadata("response_size", BIGINT),
                new ColumnMetadata("time_to_last_byte", BIGINT),
                new ColumnMetadata("trace_token", createUnboundedVarcharType()));

        private static final String TABLE_NAME = "http_request_log";

        public static OptionalInt getTimestampColumn()
        {
            return OptionalInt.of(0);
        }

        public static OptionalInt getServerAddressColumn()
        {
            return OptionalInt.of(-1);
        }
    }
    */
}
