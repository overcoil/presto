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

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.HiveTransactionHandle;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class DeltaConnector
        implements Connector
{
    private static final Logger log = Logger.get(DeltaConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final DeltaTransactionManager transactionManager;
    private final DeltaMetadataFactory metadataFactory;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorNodePartitioningProvider nodePartitioningProvider;

    // DeltaConnector follows Iceberg's example of using an internal factory that performs custom class loading.
    public DeltaConnector(
            LifeCycleManager lifeCycleManager,
            DeltaTransactionManager transactionManager,
            DeltaMetadataFactory metadataFactory,
            ConnectorSplitManager splitManager,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorNodePartitioningProvider nodePartitioningProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        // Delta's snapshot is at least READ_COMMITTED and possibly SERIALIZABLE;
        // I made the conservative choice here.
        checkConnectorSupports(READ_COMMITTED, isolationLevel);

        ConnectorTransactionHandle transaction = new HiveTransactionHandle();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            transactionManager.put(transaction, metadataFactory.create());
        }
        return transaction;
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        transactionManager.remove(transaction);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        ConnectorMetadata metadata = transactionManager.get(transaction);
        return new ClassLoaderSafeConnectorMetadata(metadata, getClass().getClassLoader());
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return nodePartitioningProvider;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
