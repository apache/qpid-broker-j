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

import static org.apache.qpid.systests.JmsTestBase.DEFAULT_BROKER_CONFIG;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.xml.bind.DatatypeConverter;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhostnode.berkeleydb.BDBVirtualHostNode;
import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.tests.utils.ConfigItem;
import org.apache.qpid.tests.utils.RunBrokerAdmin;

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
@ConfigItem(name = "qpid.initialConfigurationLocation", value = DEFAULT_BROKER_CONFIG )
public class BDBAMQP10V0UpgradeTest extends UpgradeTestBase
{
    private static final long EXPECTED_MESSAGE_LENGTH = 256 * 1024;

    @BeforeClass
    public static void verifyClient()
    {
        assumeThat(System.getProperty("virtualhostnode.type", "BDB"), is(equalTo("BDB")));
        assumeThat(getProtocol(), is(equalTo(Protocol.AMQP_1_0)));
    }

    @Test
    public void testRecoverAmqpV0Message() throws Exception
    {
        Connection connection = getConnectionBuilder().setVirtualHost("test").build();
        try
        {
            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("queue");
            MessageConsumer consumer = session.createConsumer(queue);

            Message message = consumer.receive(getReceiveTimeout());
            assertThat("Recovered message not received", message, is(instanceOf(BytesMessage.class)));
            BytesMessage bytesMessage = ((BytesMessage) message);

            long length = bytesMessage.getBodyLength();
            String expectedContentHash = message.getStringProperty("sha256hash");
            byte[] content = new byte[(int) length];
            bytesMessage.readBytes(content);

            assertThat("Unexpected content length",  length, is(equalTo(EXPECTED_MESSAGE_LENGTH)));
            assertThat("Message should carry expectedShaHash property", expectedContentHash, is(notNullValue()));

            String contentHash = computeContentHash(content);
            assertThat("Unexpected content hash", expectedContentHash, is(equalTo(contentHash)));
            session.commit();
        }
        finally
        {
            connection.close();
        }
    }

    private String computeContentHash(final byte[] content) throws Exception
    {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(content);
        return DatatypeConverter.printHexBinary(hash);
    }

    @Override
    String getOldStoreResourcePath()
    {
        return "upgrade/bdbstore-v9-amqp10v0/test-store/00000000.jdb";
    }
}
