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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

// this is used by Guice to initialize each configured catalog
final class DeltaClient
{
    private final String location;
    private final Optional<String> table;

    @JsonCreator
    public DeltaClient(
            @JsonProperty("location") String location,
            @JsonProperty("table") Optional<String> table)
    {
        requireNonNull(location, "location is null");
        requireNonNull(table, "table is null");

        this.location = location;
        this.table = table;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public Optional<String> getTable()
    {
        return table;
    }
}
