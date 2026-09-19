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

package org.apache.qpid.server.management.amqp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.Subject;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.connection.SessionPrincipal;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.plugin.SystemAddressSpaceCreator;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.security.SubjectExecutionContext;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.util.GZIPUtils.GZIPInflationLimitException;

public class ManagementAddressSpaceTest
{
    @Test
    public void createProxyNodeWithoutSubjectReturnsNull() throws Exception
    {
        final ManagementAddressSpace addressSpace = new ManagementAddressSpace(createAddressSpaceRegistry());
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, "proxy-node");

        final MessageSource source = SubjectExecutionContext.withSubject(null, () ->
                addressSpace.createMessageSource(MessageSource.class, attributes));
        final MessageDestination destination = SubjectExecutionContext.withSubject(null, () ->
                addressSpace.createMessageDestination(MessageDestination.class, attributes));

        assertNull(source, "Expected no source without subject");
        assertNull(destination, "Expected no destination without subject");
    }

    @Test
    public void proxyLookupWithoutSubjectReturnsNull() throws Exception
    {
        final ManagementAddressSpace addressSpace = new ManagementAddressSpace(createAddressSpaceRegistry());
        final Subject validSubject = createSubjectWithSessionPrincipal();
        final String proxyName = "proxy-node";
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, proxyName);

        final MessageSource createdSource = SubjectExecutionContext.withSubject(validSubject, () ->
                addressSpace.createMessageSource(MessageSource.class, attributes));
        assertNotNull(createdSource, "Expected proxy source in valid subject context");

        final MessageSource sourceWithoutSubject = SubjectExecutionContext.withSubject(null, () ->
                addressSpace.getAttainedMessageSource(proxyName));
        final MessageDestination destinationWithoutSubject = SubjectExecutionContext.withSubject(null, () ->
                addressSpace.getAttainedMessageDestination(proxyName, false));

        assertNull(sourceWithoutSubject, "Proxy source should not resolve without subject");
        assertNull(destinationWithoutSubject, "Proxy destination should not resolve without subject");
    }

    @Test
    public void managementConversionUsesConnectionDecompressionLimit() throws Exception
    {
        final AMQPConnection<?> connection = mock(AMQPConnection.class);
        when(connection.getMaxMessageDecompressionSize()).thenReturn(1024);
        final Subject subject = new Subject(false, Set.of(new ConnectionPrincipal(connection)), Set.of(), Set.of());
        final ManagementAddressSpace addressSpace = new ManagementAddressSpace(createAddressSpaceRegistry());

        final int maximumSize = SubjectExecutionContext.withSubject(subject, () ->
                addressSpace.getManagementNode().getMaximumMessageDecompressionSize());

        assertEquals(1024, maximumSize);
    }

    @Test
    public void managementConversionWithoutConnectionUsesBrokerDecompressionLimit() throws Exception
    {
        final Broker broker = createBroker();
        when(broker.getContextValue(Integer.class, Connection.MAX_MESSAGE_DECOMPRESSION_SIZE)).thenReturn(2048);
        when(broker.getContextValue(Integer.class, Connection.MAX_MESSAGE_SIZE)).thenReturn(4096);
        final ManagementAddressSpace addressSpace = new ManagementAddressSpace(createAddressSpaceRegistry(broker));

        final int maximumSize = SubjectExecutionContext.withSubject(null, () ->
                addressSpace.getManagementNode().getMaximumMessageDecompressionSize());

        assertEquals(2048, maximumSize);
    }

    @Test
    public void decompressionLimitFailureIsContainedAndClosesConnection() throws Exception
    {
        final AMQPConnection<?> connection = mock(AMQPConnection.class);
        final Subject subject = new Subject(false, Set.of(new ConnectionPrincipal(connection)), Set.of(), Set.of());
        final ManagementAddressSpace addressSpace = new ManagementAddressSpace(createAddressSpaceRegistry());
        final MessageConversionException exception = new MessageConversionException("limit exceeded",
                new GZIPInflationLimitException());

        final boolean handled = SubjectExecutionContext.withSubject(subject, () ->
                addressSpace.getManagementNode().handleMessageConversionException(exception));

        assertTrue(handled);
        verify(connection).sendConnectionCloseAsync(AMQPConnection.CloseReason.RESOURCE_LIMIT, exception.getMessage());
    }

    private static SystemAddressSpaceCreator.AddressSpaceRegistry createAddressSpaceRegistry()
    {
        return createAddressSpaceRegistry(createBroker());
    }

    private static Broker createBroker()
    {
        final Broker broker = mock(Broker.class);
        when(broker.getId()).thenReturn(UUID.randomUUID());
        when(broker.getModel()).thenReturn(BrokerModel.getInstance());
        when(broker.getCategoryClass()).thenReturn(Broker.class);
        when(broker.getTypeClass()).thenReturn(Broker.class);
        return broker;
    }

    private static SystemAddressSpaceCreator.AddressSpaceRegistry createAddressSpaceRegistry(final Broker broker)
    {
        final SystemAddressSpaceCreator.AddressSpaceRegistry registry =
                mock(SystemAddressSpaceCreator.AddressSpaceRegistry.class);
        when(registry.getBroker()).thenReturn(broker);
        return registry;
    }

    private static Subject createSubjectWithSessionPrincipal()
    {
        final AMQPSession<?, ?> session = mock(AMQPSession.class);
        final Object connectionReference = new Object();
        final AMQPConnection connection = mock(AMQPConnection.class);

        when(session.getConnectionReference()).thenReturn(connectionReference);
        when(session.getAMQPConnection()).thenReturn(connection);
        when(session.getId()).thenReturn(UUID.randomUUID());

        final SessionPrincipal sessionPrincipal = new SessionPrincipal(session);
        return new Subject(false, Set.of(sessionPrincipal), Set.of(), Set.of());
    }
}
