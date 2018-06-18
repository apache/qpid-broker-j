/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.server.protocol.v1_0;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.txn.ServerLocalTransaction;

public class TransactionDestinationRegistry
{
    private final Map<ServerLocalTransaction, Set<MessageDestination>> _transactions = new ConcurrentHashMap<>();

    public void register(ServerLocalTransaction tx, MessageDestination destination)
    {
        register(tx, Collections.singleton(destination));
    }

    public void register(ServerLocalTransaction tx, Set<MessageDestination> destinations)
    {
        Set<MessageDestination> prev = _transactions.putIfAbsent(tx, destinations);
        if (prev == null)
        {
            tx.getCompletionFuture().addListener(() -> _transactions.remove(tx), MoreExecutors.directExecutor());
        }
        else
        {
            _transactions.merge(tx,
                                destinations,
                                (old, newVal) -> Stream.of(old, newVal)
                                                       .flatMap(Collection::stream)
                                                       .collect(Collectors.toSet()));
        }
    }

    public ListenableFuture<Void> destinationRemoved(MessageDestination destination, LinkEndpoint linkEndpoint)
    {
        final SettableFuture<Void> result = SettableFuture.create();
        linkEndpoint.getSession().getConnection().doOnIOThreadAsync(() -> {
            ListenableFuture<Void> txResult = destinationRemoved(destination);
            Futures.addCallback(txResult, new FutureCallback<Void>()
            {
                @Override
                public void onSuccess(final Void r)
                {
                    try
                    {
                        result.set(null);
                    }
                    finally
                    {
                        Error e = new Error(AmqpError.RESOURCE_DELETED,
                                            String.format("Destination '%s' has been removed.",
                                                          destination.getName()));
                        linkEndpoint.close(e);
                    }
                }

                @Override
                public void onFailure(final Throwable t)
                {
                    result.setException(t);
                }
            }, MoreExecutors.directExecutor());
        });
        return result;
    }

    public void clear()
    {
        _transactions.clear();
    }

    private ListenableFuture<Void> destinationRemoved(MessageDestination destination)
    {
        Set<ListenableFuture<Void>> txs = _transactions.entrySet()
                                                       .stream()
                                                       .filter(e -> e.getValue().contains(destination))
                                                       .map(Map.Entry::getKey)
                                                       .filter(t -> !t.isComplete() && !t.setRollbackOnly())
                                                       .map(ServerLocalTransaction::getCompletionFuture)
                                                       .collect(Collectors.toSet());
        if (txs.isEmpty())
        {
            return Futures.immediateFuture(null);
        }
        return Futures.transform(Futures.allAsList(txs), input -> null, MoreExecutors.directExecutor());
    }
}
