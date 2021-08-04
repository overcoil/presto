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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

/*
  TODO: reformat

  Location specifies a path to either a Delta table directory (a folder that contains _delta_log)
   or a directory containing one or more such directory.

  Table specifies a sub-path within location
 */
public class DeltaConfig
{
    private String deltaLocation = "/default/path/to";
    private String deltaTable = "delta-table-dir";

    public String getDeltaLocation()
    {
        return deltaLocation;
    }
    public String getDeltaTable()
    {
        return deltaTable;
    }

    @Config("presto-delta.location")
    @ConfigDescription("Path for Delta table")
    public DeltaConfig setDeltaLocation(String path)
    {
        this.deltaLocation = path;
        return this;
    }

    @Config("presto-delta.table")
    @ConfigDescription("Delta table name")
    public DeltaConfig setDeltaTable(String n)
    {
        this.deltaTable = n;
        return this;
    }
}
