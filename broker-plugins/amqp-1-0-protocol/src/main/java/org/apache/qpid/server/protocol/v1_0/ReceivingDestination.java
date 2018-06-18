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

import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.txn.ServerTransaction;

public interface ReceivingDestination
{
    Symbol REJECT_UNROUTABLE = Symbol.valueOf("REJECT_UNROUTABLE");
    Symbol DISCARD_UNROUTABLE = Symbol.valueOf("DISCARD_UNROUTABLE");

    Symbol[] getCapabilities();

    Collection<BaseQueue> send(final ServerMessage<?> message,
                               final ServerTransaction txn,
                               final SecurityToken securityToken) throws UnroutableMessageException;

    int getCredit();

    String getAddress();

    MessageDestination getMessageDestination();
}
