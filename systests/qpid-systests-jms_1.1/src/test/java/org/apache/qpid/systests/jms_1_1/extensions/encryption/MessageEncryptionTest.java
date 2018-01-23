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
package org.apache.qpid.systests.jms_1_1.extensions.encryption;

import static org.apache.qpid.systests.jms_1_1.extensions.tls.TlsTest.TEST_PROFILE_RESOURCE_BASE;
import static org.apache.qpid.test.utils.TestSSLConstants.BROKER_PEERSTORE_PASSWORD;
import static org.apache.qpid.test.utils.TestSSLConstants.KEYSTORE;
import static org.apache.qpid.test.utils.TestSSLConstants.KEYSTORE_PASSWORD;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.security.FileTrustStore;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.server.virtualhostnode.JsonVirtualHostNodeImpl;
import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.test.utils.TestSSLConstants;

public class MessageEncryptionTest extends JmsTestBase
{
    private static final String TEST_MESSAGE_TEXT = "test message";
    private static final String ENCRYPTED_RECIPIENTS = "'CN=app1@acme.org, OU=art, O=acme, L=Toronto, ST=ON, C=CA'";
    private static final String QUEUE_ADDRESS_WITH_SEND_ENCRYPTED =
            "ADDR: %s ;  {x-send-encrypted : true, x-encrypted-recipients : " + ENCRYPTED_RECIPIENTS + "}";
    private static final String QUEUE_BURL_WITH_SEND_ENCRYPTED =
            "BURL:direct:///%s/%s?sendencrypted='true'&encryptedrecipients=" + ENCRYPTED_RECIPIENTS;
    private static final String BROKER_PEERSTORE = TEST_PROFILE_RESOURCE_BASE + TestSSLConstants.BROKER_PEERSTORE;

    @Before
    public void setUp() throws Exception
    {
        assumeThat("AMQP 1.0 client does not support compression yet",
                   getProtocol(),
                   is(not(equalTo(Protocol.AMQP_1_0))));
        assumeThat("Strong encryption is not enabled",
                   isStrongEncryptionEnabled(),
                   is(equalTo(Boolean.TRUE)));
    }

    @Test
    public void testEncryptionUsingMessageHeader() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection producerConnection =
                getConnectionBuilder().setEncryptionTrustStore(BROKER_PEERSTORE)
                                      .setEncryptionTrustStorePassword(BROKER_PEERSTORE_PASSWORD)
                                      .build();
        try
        {
            Connection recvConnection = getConnectionBuilder().setEncryptionKeyStore(KEYSTORE)
                                                              .setEncryptionKeyStorePassword(KEYSTORE_PASSWORD)
                                                              .build();
            try
            {
                recvConnection.start();
                final Session recvSession = recvConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageConsumer consumer = recvSession.createConsumer(queue);

                final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageProducer producer = prodSession.createProducer(queue);

                Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

                message.setBooleanProperty("x-qpid-encrypt", true);
                message.setStringProperty("x-qpid-encrypt-recipients",
                                          "cn=app1@acme.org,ou=art,o=acme,l=toronto,st=on,c=ca");

                producer.send(message);

                Message receivedMessage = consumer.receive(getReceiveTimeout());
                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
                assertEquals(TEST_MESSAGE_TEXT, ((TextMessage) message).getText());
            }
            finally
            {
                recvConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    @Test
    public void testEncryptionFromADDRAddress() throws Exception
    {
        assumeThat("Tests legacy client address syntax",
                   getProtocol(),
                   is(not(equalTo(Protocol.AMQP_1_0))));

        String queueName = getTestName();
        Queue queue = createQueue(queueName);
        Connection producerConnection =
                getConnectionBuilder().setEncryptionTrustStore(BROKER_PEERSTORE)
                                      .setEncryptionTrustStorePassword(BROKER_PEERSTORE_PASSWORD)
                                      .build();
        try
        {
            Connection recvConnection = getConnectionBuilder().setEncryptionKeyStore(KEYSTORE)
                                                              .setEncryptionKeyStorePassword(KEYSTORE_PASSWORD)
                                                              .build();
            try
            {
                recvConnection.start();
                final Session recvSession = recvConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                final MessageConsumer consumer = recvSession.createConsumer(queue);

                final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Queue prodQueue = prodSession.createQueue(String.format(QUEUE_ADDRESS_WITH_SEND_ENCRYPTED, queueName));
                final MessageProducer producer = prodSession.createProducer(prodQueue);

                Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

                producer.send(message);

                Message receivedMessage = consumer.receive(getReceiveTimeout());
                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
                assertEquals(TEST_MESSAGE_TEXT, ((TextMessage) message).getText());
            }
            finally
            {
                recvConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    @Test
    public void testEncryptionFromBURLAddress() throws Exception
    {
        assumeThat("Tests legacy client BURL syntax",
                   getProtocol(),
                   is(not(equalTo(Protocol.AMQP_1_0))));

        String queueName = getTestName();
        Queue queue = createQueue(queueName);
        Connection producerConnection =
                getConnectionBuilder().setEncryptionTrustStore(BROKER_PEERSTORE)
                                      .setEncryptionTrustStorePassword(BROKER_PEERSTORE_PASSWORD)
                                      .build();
        try
        {
            Connection recvConnection = getConnectionBuilder().setEncryptionKeyStore(KEYSTORE)
                                                              .setEncryptionKeyStorePassword(KEYSTORE_PASSWORD)
                                                              .build();
            try
            {
                recvConnection.start();
                final Session recvSession = recvConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                final MessageConsumer consumer = recvSession.createConsumer(queue);

                final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Queue prodQueue =
                        prodSession.createQueue(String.format(QUEUE_BURL_WITH_SEND_ENCRYPTED, queueName, queueName));
                final MessageProducer producer = prodSession.createProducer(prodQueue);

                Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

                producer.send(message);

                Message receivedMessage = consumer.receive(getReceiveTimeout());
                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
                assertEquals(TEST_MESSAGE_TEXT, ((TextMessage) message).getText());
            }
            finally
            {
                recvConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    @Test
    public void testBrokerAsTrustStoreProvider() throws Exception
    {
        String peerstore = "peerstore";
        addPeerStoreToBroker(peerstore, Collections.emptyMap());
        Queue queue = createQueue(getTestName());
        Connection producerConnection =
                getConnectionBuilder().setEncryptionRemoteTrustStore("$certificates%5c/" + peerstore).build();
        try
        {
            Connection recvConnection = getConnectionBuilder().setEncryptionKeyStore(KEYSTORE)
                                                              .setEncryptionKeyStorePassword(KEYSTORE_PASSWORD)
                                                              .build();
            try
            {
                recvConnection.start();
                final Session recvSession = recvConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageConsumer consumer = recvSession.createConsumer(queue);

                final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageProducer producer = prodSession.createProducer(queue);

                Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

                message.setBooleanProperty("x-qpid-encrypt", true);
                message.setStringProperty("x-qpid-encrypt-recipients",
                                          "cn=app1@acme.org,ou=art,o=acme,l=toronto,st=on,c=ca");

                producer.send(message);

                Message receivedMessage = consumer.receive(getReceiveTimeout());
                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
                assertEquals(TEST_MESSAGE_TEXT, ((TextMessage) message).getText());
            }
            finally
            {
                recvConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    @Test
    public void testBrokerStoreProviderWithExcludedVirtualHostNode() throws Exception
    {
        String testName = getTestName();

        String excludedVirtualHostNodeName = "vhn_" + testName;
        createTestVirtualHostNode(excludedVirtualHostNodeName);
        String peerstoreName = "peerstore_" + testName;
        addPeerStoreToBroker(peerstoreName, Collections.singletonMap("excludedVirtualHostNodeMessageSources",
                                                                     "[\"" + excludedVirtualHostNodeName + "\"]"));

        Queue queue = createQueue(excludedVirtualHostNodeName, testName);
        Connection producerConnection =
                getConnectionBuilder().setEncryptionRemoteTrustStore("$certificates/" + peerstoreName)
                                      .setVirtualHost(excludedVirtualHostNodeName)
                                      .build();
        try
        {

            final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = prodSession.createProducer(queue);

            Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);
            message.setBooleanProperty("x-qpid-encrypt", true);
            message.setStringProperty("x-qpid-encrypt-recipients",
                                      "cn=app1@acme.org,ou=art,o=acme,l=toronto,st=on,c=ca");

            try
            {
                producer.send(message);
                fail("Should not be able to send message");
            }
            catch (JMSException e)
            {
                assertTrue("Wrong exception cause: " + e.getCause(), e.getCause() instanceof CertificateException);
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    @Test
    public void testBrokerStoreProviderWithIncludedVirtualHostNode() throws Exception
    {
        String testName = getTestName();

        String includeVirtualHostNodeName = "vhn_" + testName;
        createTestVirtualHostNode(includeVirtualHostNodeName);

        String peerStoreName = "peerstore_" + testName;
        final Map<String, Object> additionalPeerStoreAttributes = new HashMap<>();
        String messageSources = "[\"" + includeVirtualHostNodeName + "\"]";
        additionalPeerStoreAttributes.put("includedVirtualHostNodeMessageSources", messageSources);
        // this is deliberate to test that the include list takes precedence
        additionalPeerStoreAttributes.put("excludedVirtualHostNodeMessageSources", messageSources);
        addPeerStoreToBroker(peerStoreName, additionalPeerStoreAttributes);

        Queue queue = createQueue(includeVirtualHostNodeName, testName);

        Connection successfulProducerConnection =
                getConnectionBuilder().setEncryptionRemoteTrustStore("$certificates/" + peerStoreName)
                                      .setVirtualHost(includeVirtualHostNodeName)
                                      .build();
        try
        {

            Connection failingProducerConnection = getConnectionBuilder().setVirtualHost(includeVirtualHostNodeName)
                                                                         .setEncryptionRemoteTrustStore("$certificates/"
                                                                                                        + peerStoreName)
                                                                         .build();

            final Session successfulSession =
                    successfulProducerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer successfulProducer = successfulSession.createProducer(queue);
            final Session failingSession = failingProducerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer failingProducer = failingSession.createProducer(queue);

            Message message = successfulSession.createTextMessage(TEST_MESSAGE_TEXT);
            message.setBooleanProperty("x-qpid-encrypt", true);
            message.setStringProperty("x-qpid-encrypt-recipients",
                                      "cn=app1@acme.org,ou=art,o=acme,l=toronto,st=on,c=ca");

            try
            {
                failingProducer.send(message);
                fail("Should not be able to send message");
            }
            catch (JMSException e)
            {
                assertTrue("Wrong exception cause: " + e.getCause(), e.getCause() instanceof CertificateException);
            }

            successfulProducer.send(message);
        }
        finally
        {
            successfulProducerConnection.close();
        }
    }

    private void addPeerStoreToBroker(final String peerStoreName,
                                      final Map<String, Object> additionalAttributes) throws Exception
    {
        Map<String, Object> peerStoreAttributes = new HashMap<>();
        peerStoreAttributes.put("name", peerStoreName);
        peerStoreAttributes.put("storeUrl", BROKER_PEERSTORE);
        peerStoreAttributes.put("password", BROKER_PEERSTORE_PASSWORD);
        peerStoreAttributes.put("type", "FileTrustStore");
        peerStoreAttributes.put("qpid-type", "FileTrustStore");
        peerStoreAttributes.put("exposedAsMessageSource", true);
        peerStoreAttributes.putAll(additionalAttributes);

        createEntity(peerStoreName, FileTrustStore.class.getName(), peerStoreAttributes);
    }

    private void createTestVirtualHostNode(final String excludedVirtualHostNodeName) throws Exception
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("object-type", JsonVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE);
        attributes.put("type", JsonVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE);
        attributes.put("virtualHostInitialConfiguration",
                       String.format("{\"type\": \"%s\"}", TestMemoryVirtualHost.VIRTUAL_HOST_TYPE));

        createEntity(excludedVirtualHostNodeName, "org.apache.qpid.JsonVirtualHostNode", attributes);
    }

    private void createEntity(final String entityName,
                              final String entityType,
                              final Map<String, Object> attributes) throws Exception
    {
        Connection connection = getConnectionBuilder().setVirtualHost("$management").build();
        try
        {
            connection.start();
            createEntity(entityName, entityType, attributes, connection);
        }
        finally
        {
            connection.close();
        }
    }

    private boolean isStrongEncryptionEnabled() throws NoSuchAlgorithmException
    {
        return Cipher.getMaxAllowedKeyLength("AES") >= 256;
    }
}
