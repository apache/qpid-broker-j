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
package org.apache.qpid.server.model;

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.LinkModel;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.txn.DtxRegistry;
import org.apache.qpid.server.virtualhost.ConnectionEstablishmentPolicy;
import org.apache.qpid.server.virtualhost.LinkRegistryModel;

public interface NamedAddressSpace extends Named
{

    UUID getId();

    MessageSource getAttainedMessageSource(String name);

    MessageDestination getAttainedMessageDestination(String name, boolean mayCreate);

    boolean registerConnection(AMQPConnection<?> connection,
                               final ConnectionEstablishmentPolicy connectionEstablishmentPolicy);
    void deregisterConnection(AMQPConnection<?> connection);


    String getRedirectHost(AmqpPort<?> port);

    Principal getPrincipal();

    boolean isActive();

    MessageDestination getDefaultDestination();

    <T extends LinkModel> T getSendingLink(String remoteContainerId, String linkName);
    <T extends LinkModel> T getReceivingLink(String remoteContainerId, String linkName);
    @Deprecated
    <T extends LinkModel> Collection<T> findSendingLinks(final Pattern containerIdPattern,
                                                         final Pattern linkNamePattern);

    <T extends LinkModel> void visitSendingLinks(final LinkRegistryModel.LinkVisitor<T> visitor);

    boolean authoriseCreateConnection(AMQPConnection<?> connection);

    DtxRegistry getDtxRegistry();

    MessageStore getMessageStore();

    <T extends MessageSource> T createMessageSource(Class<T> clazz, Map<String,Object> attributes);
    <T extends MessageDestination> T createMessageDestination(Class<T> clazz, Map<String,Object> attributes);

    boolean hasMessageSources();

    Collection<? extends Connection<?>> getConnections();

    List<String> getGlobalAddressDomains();

    String getLocalAddress(String routingAddress);
}
