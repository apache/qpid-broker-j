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
package org.apache.qpid.server.store.berkeleydb;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.MessageDigest;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.xml.bind.DatatypeConverter;

import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.virtualhostnode.berkeleydb.BDBVirtualHostNode;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;
import org.apache.qpid.server.util.FileUtils;

/**
 *
 * The store was formed with an Qpid JMS Client 0.26 and Qpid Broker v6.1.4 configured to use a BDB virtualhostnode
 * and provided store.
 *
 * byte[] content = new byte[256*1024];
 * IntStream.range(0, content.length).forEachOrdered(i -> content[i] = (byte) (i % 256));
 * BytesMessage message = session.createBytesMessage();
 * message.writeBytes(content);
 * message.setStringProperty("sha256hash", DatatypeConverter.printHexBinary(MessageDigest.getInstance("SHA-256").digest(content)));
 * messageProducer.send(message);
 *
 */
public class BDBAMQP10V0UpgradeTest extends QpidBrokerTestCase
{
    private static final int EXPECTED_MESSAGE_LENGTH = 256 * 1024;

    private String _storeLocation;

    @Override
    public void setUp() throws Exception
    {
        _storeLocation = Files.createTempDirectory("qpid-work-" + getClassQualifiedTestName() + "-bdb-store").toString();
        TestBrokerConfiguration brokerConfiguration = getDefaultBrokerConfiguration();
        brokerConfiguration.setObjectAttribute(VirtualHostNode.class, TestBrokerConfiguration.ENTRY_NAME_VIRTUAL_HOST, BDBVirtualHostNode.STORE_PATH, _storeLocation );

        //Clear the two target directories if they exist.
        File directory = new File(_storeLocation);
        if (directory.exists() && directory.isDirectory())
        {
            FileUtils.delete(directory, true);
        }
        directory.mkdirs();

        // copy store files
        InputStream src = getClass().getClassLoader().getResourceAsStream("upgrade/bdbstore-v9-amqp10v0/test-store/00000000.jdb");
        FileUtils.copy(src, new File(_storeLocation, "00000000.jdb"));

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            super.tearDown();
        }
        finally
        {
            FileUtils.delete(new File(_storeLocation), true);
        }
    }

    public void testRecoverAmqpV0Message() throws Exception
    {
        Connection connection = getConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = createTestQueue(session, "queue");
        MessageConsumer consumer = session.createConsumer(queue);

        Message message = consumer.receive(getReceiveTimeout());
        assertNotNull("Recovered message not received", message);
        assertTrue(message instanceof BytesMessage);
        BytesMessage bytesMessage = ((BytesMessage) message);

        long length = bytesMessage.getBodyLength();
        String expectedContentHash = message.getStringProperty("sha256hash");
        byte[] content = new byte[(int) length];
        bytesMessage.readBytes(content);

        assertEquals("Unexpected content length", EXPECTED_MESSAGE_LENGTH, length);
        assertNotNull("Message should carry expectedShaHash property", expectedContentHash);

        String contentHash = computeContentHash(content);
        assertEquals("Unexpected content hash", expectedContentHash, contentHash);
        session.commit();
    }

    private String computeContentHash(final byte[] content) throws Exception
    {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(content);
        return DatatypeConverter.printHexBinary(hash);
    }
}
