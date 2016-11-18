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
package org.apache.qpid.systest.messageencryption;

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

import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.client.message.JMSBytesMessage;
import org.apache.qpid.client.message.JMSTextMessage;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestSSLConstants;

public class MessageEncryptionTest extends QpidBrokerTestCase implements TestSSLConstants
{

    public static final String TEST_MESSAGE_TEXT = "test message";
    public static final String EXCLUDED_VIRTUAL_HOST_NODE_NAME = "excludedVirtualHostNode";
    public static final String INCLUDED_VIRTUAL_HOST_NODE_NAME = "includedVirtualHostNode";

    @Override
    public void startDefaultBroker() throws Exception
    {
        // tests start broker
    }

    public void testEncryptionUsingMessageHeader() throws Exception
    {
        if(isStrongEncryptionEnabled() && !isCppBroker())
        {
            super.startDefaultBroker();
            Map<String, String> prodConnOptions = new HashMap<>();
            prodConnOptions.put("encryption_trust_store", BROKER_PEERSTORE);
            prodConnOptions.put("encryption_trust_store_password", BROKER_PEERSTORE_PASSWORD);
            Connection producerConnection = getConnectionWithOptions(prodConnOptions);


            Map<String, String> recvConnOptions = new HashMap<>();
            recvConnOptions.put("encryption_key_store", KEYSTORE);
            recvConnOptions.put("encryption_key_store_password", KEYSTORE_PASSWORD);
            Connection recvConnection = getConnectionWithOptions(recvConnOptions);

            recvConnection.start();
            final Session recvSession = recvConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = getTestQueue();
            final MessageConsumer consumer = recvSession.createConsumer(queue);


            final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = prodSession.createProducer(queue);

            Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

            message.setBooleanProperty("x-qpid-encrypt", true);
            message.setStringProperty("x-qpid-encrypt-recipients",
                                      "cn=app1@acme.org,ou=art,o=acme,l=toronto,st=on,c=ca");

            producer.send(message);


            Message receivedMessage = consumer.receive(1000l);
            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof JMSTextMessage);
            assertEquals(TEST_MESSAGE_TEXT, ((JMSTextMessage) message).getText());
        }
    }

    public void testEncryptionFromADDRAddress() throws Exception
    {
        if(isStrongEncryptionEnabled() && !isCppBroker())
        {
            super.startDefaultBroker();
            Map<String, String> prodConnOptions = new HashMap<>();
            prodConnOptions.put("encryption_trust_store", BROKER_PEERSTORE);
            prodConnOptions.put("encryption_trust_store_password", BROKER_PEERSTORE_PASSWORD);
            Connection producerConnection = getConnectionWithOptions(prodConnOptions);


            Map<String, String> recvConnOptions = new HashMap<>();
            recvConnOptions.put("encryption_key_store", KEYSTORE);
            recvConnOptions.put("encryption_key_store_password", KEYSTORE_PASSWORD);
            Connection recvConnection = getConnectionWithOptions(recvConnOptions);

            recvConnection.start();
            final Session recvSession = recvConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = getTestQueue();
            final MessageConsumer consumer = recvSession.createConsumer(queue);


            final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue prodQueue = prodSession.createQueue("ADDR: " + getTestQueueName() + " ;  {x-send-encrypted : true, x-encrypted-recipients : 'CN=app1@acme.org, OU=art, O=acme, L=Toronto, ST=ON, C=CA'} ");
            final MessageProducer producer = prodSession.createProducer(prodQueue);

            Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

            producer.send(message);


            Message receivedMessage = consumer.receive(1000l);
            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof JMSTextMessage);
            assertEquals(TEST_MESSAGE_TEXT, ((JMSTextMessage) message).getText());
        }
    }

    public void testEncryptionFromBURLAddress() throws Exception
    {
        if(isStrongEncryptionEnabled() && !isCppBroker())
        {
            super.startDefaultBroker();
            Map<String, String> prodConnOptions = new HashMap<>();
            prodConnOptions.put("encryption_trust_store", BROKER_PEERSTORE);
            prodConnOptions.put("encryption_trust_store_password", BROKER_PEERSTORE_PASSWORD);
            Connection producerConnection = getConnectionWithOptions(prodConnOptions);


            Map<String, String> recvConnOptions = new HashMap<>();
            recvConnOptions.put("encryption_key_store", KEYSTORE);
            recvConnOptions.put("encryption_key_store_password", KEYSTORE_PASSWORD);
            Connection recvConnection = getConnectionWithOptions(recvConnOptions);

            recvConnection.start();
            final Session recvSession = recvConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = getTestQueue();
            final MessageConsumer consumer = recvSession.createConsumer(queue);


            final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue prodQueue = prodSession.createQueue("BURL:direct:///"
                                                      + getTestQueueName()
                                                      + "/"
                                                      + getTestQueueName()
                                                      + "?sendencrypted='true'&encryptedrecipients='CN=app1@acme.org, OU=art, O=acme, L=Toronto, ST=ON, C=CA'");
            final MessageProducer producer = prodSession.createProducer(prodQueue);

            Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

            producer.send(message);


            Message receivedMessage = consumer.receive(1000l);
            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof JMSTextMessage);
            assertEquals(TEST_MESSAGE_TEXT, ((JMSTextMessage) message).getText());
        }
    }


    public void testBrokerAsTrustStoreProvider() throws Exception
    {
        if(isStrongEncryptionEnabled() && !isCppBroker())
        {
            addPeerStoreToBroker();
            super.startDefaultBroker();
            Map<String, String> prodConnOptions = new HashMap<>();
            prodConnOptions.put("encryption_remote_trust_store","$certificates%5c/peerstore");
            Connection producerConnection = getConnectionWithOptions(prodConnOptions);


            Map<String, String> recvConnOptions = new HashMap<>();
            recvConnOptions.put("encryption_key_store", KEYSTORE);
            recvConnOptions.put("encryption_key_store_password", KEYSTORE_PASSWORD);
            Connection recvConnection = getConnectionWithOptions(recvConnOptions);

            recvConnection.start();
            final Session recvSession = recvConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = getTestQueue();
            final MessageConsumer consumer = recvSession.createConsumer(queue);


            final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = prodSession.createProducer(queue);

            Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

            message.setBooleanProperty("x-qpid-encrypt", true);
            message.setStringProperty("x-qpid-encrypt-recipients",
                                      "cn=app1@acme.org,ou=art,o=acme,l=toronto,st=on,c=ca");

            producer.send(message);


            Message receivedMessage = consumer.receive(1000l);
            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof JMSTextMessage);
            assertEquals(TEST_MESSAGE_TEXT, ((JMSTextMessage) message).getText());
        }
    }

    public void testBrokerStoreProviderWithExcludedVirtualHostNode() throws Exception
    {
        if(isStrongEncryptionEnabled() && !isCppBroker())
        {
            createTestVirtualHostNode(EXCLUDED_VIRTUAL_HOST_NODE_NAME);
            addPeerStoreToBroker(Collections.<String, Object>singletonMap("excludedVirtualHostNodeMessageSources",
                                                                          EXCLUDED_VIRTUAL_HOST_NODE_NAME));
            super.startDefaultBroker();

            String connectionUrlString = "amqp://guest:guest@clientId/" + EXCLUDED_VIRTUAL_HOST_NODE_NAME
                                         + "?brokerlist='tcp://localhost:" + getDefaultAmqpPort() + "'"
                                         + "&encryption_remote_trust_store='$certificates%5c/peerstore'";
            final AMQConnectionURL connectionUrl = new AMQConnectionURL(connectionUrlString);
            Connection producerConnection = getConnection(connectionUrl);

            Queue queue = getTestQueue();
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
    }

    public void testBrokerStoreProviderWithIncludedVirtualHostNode() throws Exception
    {
        if(isStrongEncryptionEnabled() && !isCppBroker())
        {
            createTestVirtualHostNode(INCLUDED_VIRTUAL_HOST_NODE_NAME);
            final Map<String, Object> additionalPeerStoreAttributes = new HashMap<>();
            additionalPeerStoreAttributes.put("includedVirtualHostNodeMessageSources", INCLUDED_VIRTUAL_HOST_NODE_NAME);
            // this is deliberate to test that the include list takes precedence
            additionalPeerStoreAttributes.put("excludedVirtualHostNodeMessageSources", INCLUDED_VIRTUAL_HOST_NODE_NAME);
            addPeerStoreToBroker(additionalPeerStoreAttributes);
            super.startDefaultBroker();

            String connectionUrlString;

            connectionUrlString = "amqp://guest:guest@clientId/" + INCLUDED_VIRTUAL_HOST_NODE_NAME
                                  + "?brokerlist='tcp://localhost:" + getDefaultAmqpPort() + "'"
                                  + "&encryption_remote_trust_store='$certificates%5c/peerstore'";
            final AMQConnectionURL connectionUrl = new AMQConnectionURL(connectionUrlString);
            Connection successfulProducerConnection = getConnection(connectionUrl);

            Connection failingProducerConnection = getConnectionWithOptions(Collections.singletonMap("encryption_remote_trust_store",
                                                                                                     "$certificates%5c/peerstore"));

            Queue queue = getTestQueue();
            final Session successfulSession = successfulProducerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
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
    }

    public void testUnknownRecipient() throws Exception
    {

        if(isStrongEncryptionEnabled() && !isCppBroker())
        {
            addPeerStoreToBroker();
            super.startDefaultBroker();
            Map<String, String> prodConnOptions = new HashMap<>();
            prodConnOptions.put("encryption_remote_trust_store","$certificates%5c/peerstore");
            Connection producerConnection = getConnectionWithOptions(prodConnOptions);


            Map<String, String> recvConnOptions = new HashMap<>();
            recvConnOptions.put("encryption_key_store", KEYSTORE);
            recvConnOptions.put("encryption_key_store_password", KEYSTORE_PASSWORD);
            Connection recvConnection = getConnectionWithOptions(recvConnOptions);

            recvConnection.start();
            final Session recvSession = recvConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = getTestQueue();
            final MessageConsumer consumer = recvSession.createConsumer(queue);


            final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = prodSession.createProducer(queue);

            Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

            message.setBooleanProperty("x-qpid-encrypt", true);
            message.setStringProperty("x-qpid-encrypt-recipients",
                                      "cn=unknwon@acme.org,ou=art,o=acme,l=toronto,st=on,c=ca");

            try
            {
                producer.send(message);
                fail("Should not have been able to send a message to an unknown recipient");
            }
            catch(JMSException e)
            {
                // pass;
            }

        }
    }

    public void testRecipientHasNoValidCert() throws Exception
    {
        if(isStrongEncryptionEnabled() && !isCppBroker())
        {
            super.startDefaultBroker();
            Map<String, String> prodConnOptions = new HashMap<>();
            prodConnOptions.put("encryption_trust_store", BROKER_PEERSTORE);
            prodConnOptions.put("encryption_trust_store_password", BROKER_PEERSTORE_PASSWORD);
            Connection producerConnection = getConnectionWithOptions(prodConnOptions);


            Map<String, String> recvConnOptions = new HashMap<>();
            Connection recvConnection = getConnectionWithOptions(recvConnOptions);

            recvConnection.start();
            final Session recvSession = recvConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = getTestQueue();
            final MessageConsumer consumer = recvSession.createConsumer(queue);


            final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue prodQueue = prodSession.createQueue("ADDR: " + getTestQueueName() + " ;  {x-send-encrypted : true, x-encrypted-recipients : 'CN=app1@acme.org, OU=art, O=acme, L=Toronto, ST=ON, C=CA'} ");
            final MessageProducer producer = prodSession.createProducer(prodQueue);

            Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

            producer.send(message);


            Message receivedMessage = consumer.receive(1000l);
            assertNotNull(receivedMessage);
            assertFalse(receivedMessage instanceof JMSTextMessage);
            assertTrue(receivedMessage instanceof JMSBytesMessage);
        }
    }

    private void addPeerStoreToBroker()
    {
        addPeerStoreToBroker(Collections.<String, Object>emptyMap());
    }

    private void addPeerStoreToBroker(Map<String, Object> additionalAttributes)
    {
        Map<String, Object> peerStoreAttributes = new HashMap<>();
        peerStoreAttributes.put("name" , "peerstore");
        peerStoreAttributes.put("storeUrl" , "${QPID_HOME}${file.separator}..${file.separator}test-profiles${file.separator}test_resources${file.separator}ssl${file.separator}java_broker_peerstore.jks");
        peerStoreAttributes.put("password" , "password");
        peerStoreAttributes.put("type", "FileTrustStore");
        peerStoreAttributes.put("exposedAsMessageSource", true);
        peerStoreAttributes.putAll(additionalAttributes);
        getDefaultBrokerConfiguration().addObjectConfiguration(TrustStore.class, peerStoreAttributes);
    }


    private boolean isStrongEncryptionEnabled() throws NoSuchAlgorithmException
    {
        return Cipher.getMaxAllowedKeyLength("AES")>=256;
    }
}
