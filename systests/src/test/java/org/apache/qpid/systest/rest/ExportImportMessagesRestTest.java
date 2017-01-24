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
 *
 */
package org.apache.qpid.systest.rest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.test.utils.TestBrokerConfiguration;
import org.apache.qpid.util.DataUrlUtils;

public class ExportImportMessagesRestTest extends QpidRestTestCase
{

    private String _virtualHostNodeName;
    private String _virtualHostName;
    private String _queueName;
    private String _extractOpUrl;
    private String _importOpUrl;
    private Destination _jmsQueue;
    private List<Message> _expectedMessages;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        _virtualHostNodeName = getTestName() + "_node";
        _virtualHostName = getTestName() + "_host";
        _queueName = getTestQueueName();

        createVirtualHostNode(_virtualHostNodeName, _virtualHostName);

        createTestQueue(_virtualHostNodeName, _virtualHostName, _queueName);

        Connection connection = createConnection(_virtualHostName);
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        _jmsQueue = session.createQueue(isBroker10() ? _queueName : String.format("direct:////%s", _queueName));
        _expectedMessages = sendMessage(session, _jmsQueue, 1);

        connection.close();

        _extractOpUrl = String.format("virtualhost/%s/%s/exportMessageStore", _virtualHostNodeName, _virtualHostName);
        _importOpUrl = String.format("virtualhost/%s/%s/importMessageStore", _virtualHostNodeName, _virtualHostName);

    }

    @Override
    protected void customizeConfiguration() throws Exception
    {
        super.customizeConfiguration();
        getDefaultBrokerConfiguration().setObjectAttribute(Port.class, TestBrokerConfiguration.ENTRY_NAME_HTTP_PORT,
                                                           HttpPort.ALLOW_CONFIDENTIAL_OPERATIONS_ON_INSECURE_CHANNELS,
                                                           true);
    }

    public void testExtractImportEndToEnd() throws Exception
    {
        if (!isBrokerStorePersistent())
        {
            return;
        }
        changeVirtualHostState(_virtualHostNodeName, _virtualHostName, "STOPPED");

        byte[] extractedBytes = getRestTestHelper().getBytes(_extractOpUrl);
        String extractedBytesAsDataUrl = DataUrlUtils.getDataUrlForBytes(extractedBytes);

        // Delete and recreate
        deleteVirtualHostNode(_virtualHostNodeName);
        createVirtualHostNode(_virtualHostNodeName, _virtualHostName);
        createTestQueue(_virtualHostNodeName, _virtualHostName, _queueName);

        changeVirtualHostState(_virtualHostNodeName, _virtualHostName, "STOPPED");

        Map<String, Object> importArgs = Collections.<String, Object>singletonMap("source", extractedBytesAsDataUrl);
        getRestTestHelper().postJson(_importOpUrl, importArgs, Void.class);

        changeVirtualHostState(_virtualHostNodeName, _virtualHostName, "ACTIVE");

        // Verify imported message present
        Connection connection = createConnection(_virtualHostName);
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = session.createConsumer(_jmsQueue);

        final TextMessage expectedMessage = (TextMessage) _expectedMessages.get(0);
        final TextMessage receivedMessage = (TextMessage) consumer.receive(getReceiveTimeout());
        assertNotNull("Message not received", receivedMessage);
        assertEquals("Unexpected message id", expectedMessage.getJMSMessageID(), receivedMessage.getJMSMessageID());
        assertEquals("Unexpected message content", expectedMessage.getText(), receivedMessage.getText());

        connection.close();

    }

    private void createTestQueue(final String nodeName, final String hostName, final String queueName) throws Exception
    {
        Map<String, Object> queueAttributes = new HashMap<>();
        queueAttributes.put(Queue.NAME, queueName);

        getRestTestHelper().submitRequest(String.format("queue/%s/%s", nodeName, hostName),
                                          "POST",
                                          queueAttributes,
                                          HttpServletResponse.SC_CREATED);

    }

    private void changeVirtualHostState(final String virtualHostNodeName, final String virtualHostName, final String desiredState) throws IOException
    {
        Map<String, Object>
                attributes = Collections.<String, Object>singletonMap(VirtualHost.DESIRED_STATE, desiredState);
        getRestTestHelper().submitRequest(String.format("virtualhost/%s/%s", virtualHostNodeName, virtualHostName), "PUT", attributes, HttpServletResponse.SC_OK);
    }

    private void createVirtualHostNode(String virtualHostNodeName, final String virtualHostName) throws Exception
    {
        String type = getTestProfileVirtualHostNodeType();

        Map<String, Object> virtualHostAttributes = Collections.<String, Object>singletonMap(VirtualHost.NAME, virtualHostName);

        Map<String, Object> nodeAttributes = new HashMap<>();
        nodeAttributes.put(VirtualHostNode.NAME, virtualHostNodeName);
        nodeAttributes.put(VirtualHostNode.TYPE, type);
        nodeAttributes.put(VirtualHostNode.VIRTUALHOST_INITIAL_CONFIGURATION, new ObjectMapper().writeValueAsString(virtualHostAttributes));


        getRestTestHelper().submitRequest(String.format("virtualhostnode/%s", virtualHostNodeName),
                                          "PUT",
                                          nodeAttributes,
                                          HttpServletResponse.SC_CREATED);
    }

    private void deleteVirtualHostNode(String virtualHostNodeName) throws IOException
    {
        getRestTestHelper().submitRequest(String.format("virtualhostnode/%s", virtualHostNodeName), "DELETE", HttpServletResponse.SC_OK);
    }

    private Connection createConnection(final String virtualHostName) throws Exception
    {
        return getConnectionForVHost(virtualHostName);
    }
}
