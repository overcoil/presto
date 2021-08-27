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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.delta.standalone.FileFormat;
import io.delta.standalone.actions.AddFile;

import javax.inject.Inject;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

// TODO: the prototype presents a single split for each table
public class DeltaSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;

    @Inject
    public DeltaSplitManager(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        // downcast layout to to recover the target table
        DeltaTableLayoutHandle deltaTableLayoutHandle = (DeltaTableLayoutHandle) layout;
        DeltaClient deltaClient = deltaTableLayoutHandle.getTable().getDeltaClient();
        String tableName = deltaTableLayoutHandle.getTable().getTableName();

        List<ConnectorSplit> splits = new ArrayList<>();
        for (AddFile addFile : deltaClient.getSnapshotForTable(tableName).getAllFiles()) {
            // the DeltaClient holds the path to the directory which needs to be augmented with the individual AddFile's name
            splits.add(new DeltaSplit(Paths.get(deltaClient.getPathForTable(tableName), addFile.getPath()).toString(), FileFormat.PARQUET));
        }

        // this naive implementation uses FixedSplitSource and ignores partitions
        return new FixedSplitSource(splits);
    }
}
