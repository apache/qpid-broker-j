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
package org.apache.qpid.server.security;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.message.MessageContainer;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.TestMemoryMessageStore;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class TrustStoreMessageSourceTest extends UnitTestBase
{
    private TrustStoreMessageSource _trustStoreMessageSource;
    private Certificate[] _certificates;

    @BeforeEach
    public void setUp() throws Exception
    {
        final VirtualHost<?> vhost = mock(VirtualHost.class);
        final MessageStore messageStore = new TestMemoryMessageStore();
        final TrustStore<?> trustStore = mock(TrustStore.class);
        final Certificate certificate = mock(Certificate.class);
        _certificates = new Certificate[]{certificate};

        when(vhost.getMessageStore()).thenReturn(messageStore);
        when(trustStore.getState()).thenReturn(State.ACTIVE);
        when(trustStore.getCertificates()).thenReturn(_certificates);
        when(certificate.getEncoded()).thenReturn("my certificate".getBytes());
        _trustStoreMessageSource= new TrustStoreMessageSource(trustStore, vhost);
    }

    @Test
    public void testAddConsumer() throws Exception
    {
        final EnumSet<ConsumerOption> options = EnumSet.noneOf(ConsumerOption.class);
        final ConsumerTarget target = mock(ConsumerTarget.class);
        when(target.allocateCredit(any(ServerMessage.class))).thenReturn(true);

        MessageInstanceConsumer<?> consumer = _trustStoreMessageSource.addConsumer(target, null, ServerMessage.class, getTestName(), options, 0);
        final MessageContainer messageContainer = consumer.pullMessage();
        assertNotNull(messageContainer, "Could not pull message of TrustStore");
        final ServerMessage<?> message = messageContainer.getMessageInstance().getMessage();
        assertCertificates(getCertificatesFromMessage(message));
    }

    private void assertCertificates(final List<String> encodedCertificates) throws CertificateEncodingException
    {
        for (int i = 0; i < _certificates.length; ++i)
        {
            assertArrayEquals(_certificates[i].getEncoded(), encodedCertificates.get(i).getBytes(),
                    "Unexpected content");
        }
    }

    private List<String> getCertificatesFromMessage(final ServerMessage<?> message) throws ClassNotFoundException
    {
        final int bodySize = (int) message.getSize();
        final byte[] msgContent = new byte[bodySize];
        final List<String> certificates;
        final ByteArrayInputStream bytesIn;
        try (final QpidByteBuffer allData = message.getStoredMessage().getContent(0, bodySize))
        {
            assertEquals(bodySize, (long) allData.remaining(), "Unexpected message size was retrieved");
            allData.get(msgContent);
        }

        certificates = new ArrayList<>();
        bytesIn = new ByteArrayInputStream(msgContent);
        try (final ObjectInputStream is = new ObjectInputStream(bytesIn))
        {
            final ArrayList<byte[]> encodedCertificates = (ArrayList<byte[]>) is.readObject();
            for (final byte[] encodedCertificate : encodedCertificates)
            {
                certificates.add(new String(encodedCertificate));
            }
        }
        catch (IOException e)
        {
            fail("Unexpected IO Exception on operation: " + e.getMessage());
        }
        return certificates;
    }
}
