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
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

// TODO: the prototype presents a single split for each table
public class DeltaSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private final DeltaConfig config;  // should be replaced by a DeltaClient object to enable sharing across multiple configured catalogs

    @Inject
    public DeltaSplitManager(NodeManager nodeManager, DeltaConfig cfg)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.config = requireNonNull(cfg, "config is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        DeltaTableLayoutHandle layoutHandle = (DeltaTableLayoutHandle) layout;
        DeltaTableHandle table = layoutHandle.getTable();

        // TODO: use layout to prune the splits to fetch

        DeltaSplitSource splitSource = new DeltaSplitSource(config.getSnapshot().getAllFiles());
        return splitSource;
    }
}
