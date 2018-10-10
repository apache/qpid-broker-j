/*
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
 */
package org.apache.qpid.server.virtualhost;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.EnumSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.message.MessageContainer;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.TestMemoryMessageStore;
import org.apache.qpid.test.utils.UnitTestBase;

public class VirtualHostPropertiesNodeTest extends UnitTestBase
{
    private VirtualHostPropertiesNode _virtualHostPropertiesNode;

    @Before
    public void setUp() throws Exception
    {
        VirtualHost vhost = mock(VirtualHost.class);
        MessageStore messageStore = new TestMemoryMessageStore();
        when(vhost.getMessageStore()).thenReturn(messageStore);

        _virtualHostPropertiesNode = new VirtualHostPropertiesNode(vhost);
    }

    @Test
    public void testAddConsumer() throws Exception
    {
        final EnumSet<ConsumerOption> options = EnumSet.noneOf(ConsumerOption.class);
        final ConsumerTarget target = mock(ConsumerTarget.class);
        when(target.allocateCredit(any(ServerMessage.class))).thenReturn(true);

        MessageInstanceConsumer consumer = _virtualHostPropertiesNode.addConsumer(target, null, ServerMessage.class, getTestName(), options, 0);
        final MessageContainer messageContainer = consumer.pullMessage();
        assertNotNull("Could not pull message from VirtualHostPropertyNode", messageContainer);
        if (messageContainer.getMessageReference() != null)
        {
            messageContainer.getMessageReference().release();
        }
    }
}
