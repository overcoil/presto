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
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class DeltaTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final String pathName;
    private final TupleDomain<DeltaColumnHandle> predicate;

    // NB this needs to be reconstructed and is not serialized
    private final DeltaClient deltaClient;

    @JsonCreator
    public DeltaTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("pathName") String pathName,
            @JsonProperty("predicate") TupleDomain<DeltaColumnHandle> predicate)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.pathName = requireNonNull(pathName, "pathName is null");
        this.predicate = requireNonNull(predicate, "predicate is null");

        this.deltaClient = new DeltaClient(this.pathName);
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getPathName()
    {
        return pathName;
    }

    @JsonProperty
    public TupleDomain<DeltaColumnHandle> getPredicate()
    {
        return predicate;
    }

    public DeltaClient getDeltaClient()
    {
        return deltaClient;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeltaTableHandle that = (DeltaTableHandle) o;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(pathName, that.pathName);
        // TOOD: handle predicate too in the future
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, pathName);
    }

    @Override
    public String toString()
    {
        // TODO: derive current; hack to force the conversion of getSchemaTableName() to String
        return getSchemaTableName() + "@current";
    }
}
