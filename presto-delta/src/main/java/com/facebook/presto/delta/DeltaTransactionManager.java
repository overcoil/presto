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

import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class DeltaTransactionManager
{
    private final Map<ConnectorTransactionHandle, DeltaMetadata> transactions = new ConcurrentHashMap<>();

    public DeltaMetadata get(ConnectorTransactionHandle transaction)
    {
        DeltaMetadata metadata = transactions.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    public DeltaMetadata remove(ConnectorTransactionHandle transaction)
    {
        DeltaMetadata metadata = transactions.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    public void put(ConnectorTransactionHandle transaction, DeltaMetadata metadata)
    {
        ConnectorMetadata existing = transactions.putIfAbsent(transaction, metadata);
        checkState(existing == null, "transaction already exists: %s", existing);
    }
}
