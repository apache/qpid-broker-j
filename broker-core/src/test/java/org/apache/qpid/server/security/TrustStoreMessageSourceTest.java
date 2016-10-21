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

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import org.mockito.ArgumentCaptor;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.message.ConsumerOption;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.TestMemoryMessageStore;
import org.apache.qpid.test.utils.QpidTestCase;


public class TrustStoreMessageSourceTest extends QpidTestCase
{
    private TrustStoreMessageSource _trustStoreMessageSource;
    private Certificate[] _certificates;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        VirtualHost vhost = mock(VirtualHost.class);
        MessageStore messageStore = new TestMemoryMessageStore();
        TrustStore trustStore = mock(TrustStore.class);
        Certificate certificate = mock(Certificate.class);
        _certificates = new Certificate[]{certificate};

        when(vhost.getMessageStore()).thenReturn(messageStore);
        when(trustStore.getState()).thenReturn(State.ACTIVE);
        when(trustStore.getCertificates()).thenReturn(_certificates);
        when(certificate.getEncoded()).thenReturn("my certificate".getBytes());
        _trustStoreMessageSource= new TrustStoreMessageSource(trustStore, vhost);
    }

    public void testAddConsumer() throws Exception
    {
        final EnumSet<ConsumerOption> options = EnumSet.noneOf(ConsumerOption.class);
        final ConsumerTarget target = mock(ConsumerTarget.class);
        when(target.allocateCredit(any(ServerMessage.class))).thenReturn(true);

        _trustStoreMessageSource.addConsumer(target, null, ServerMessage.class, getTestName(), options, 0);

        ArgumentCaptor<MessageInstance> argumentCaptor = ArgumentCaptor.forClass(MessageInstance.class);
        verify(target).send(any(MessageInstanceConsumer.class), argumentCaptor.capture(), anyBoolean());
        final ServerMessage message = argumentCaptor.getValue().getMessage();
        assertCertificates(getCertificatesFromMessage(message));
    }

    private void assertCertificates(final List<String> encodedCertificates) throws CertificateEncodingException
    {
        for (int i = 0; i < _certificates.length; ++i)
        {
            assertArrayEquals("Unexpected content", _certificates[i].getEncoded(), encodedCertificates.get(i).getBytes());
        }
    }

    private List<String> getCertificatesFromMessage(final ServerMessage<?> message) throws ClassNotFoundException
    {
        final int bodySize = (int) message.getSize();
        byte[] msgContent = new byte[bodySize];
        final Collection<QpidByteBuffer> allData = message.getStoredMessage().getContent(0, bodySize);
        int total = 0;
        for(QpidByteBuffer b : allData)
        {
            int len = b.remaining();
            b.get(msgContent, total, len);
            b.dispose();
            total += len;
        }
        assertEquals("Unexpected message size was retrieved", bodySize, total);

        List<String> certificates = new ArrayList<>();
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(msgContent);
        try (ObjectInputStream is = new ObjectInputStream(bytesIn))
        {
            ArrayList<byte[]> encodedCertificates = (ArrayList<byte[]>) is.readObject();
            for(byte[] encodedCertificate : encodedCertificates)
            {
                certificates.add(new String(encodedCertificate));
            }
            is.close();
        }
        catch (IOException e)
        {
            fail("Unexpected IO Exception on operation: " + e.getMessage());
        }
        return certificates;
    }
}
