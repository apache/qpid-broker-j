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
package org.apache.qpid.server.message;

import java.security.AccessControlException;
import java.util.Map;

import org.apache.qpid.server.exchange.DestinationReferrer;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.PublishingLink;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.store.StorableMessageMetaData;

public interface MessageDestination extends MessageNode
{

    @Override
    String getName();

    NamedAddressSpace getAddressSpace();

    void authorisePublish(SecurityToken token, Map<String, Object> arguments) throws AccessControlException;

    /**
     * Routes a message
     *
     *
     * @param message the message to be routed
     * @param routingAddress the routing address
     * @param instanceProperties the instance properties
    */
    <M extends ServerMessage<? extends StorableMessageMetaData>> RoutingResult<M> route(M message,
                                                                                        String routingAddress,
                                                                                        InstanceProperties instanceProperties);

    boolean isDurable();

    void linkAdded(MessageSender sender, PublishingLink link);
    void linkRemoved(MessageSender sender, PublishingLink link);

    MessageDestination getAlternateBindingDestination();

    void addReference(DestinationReferrer destinationReferrer);

    void removeReference(DestinationReferrer destinationReferrer);
}
