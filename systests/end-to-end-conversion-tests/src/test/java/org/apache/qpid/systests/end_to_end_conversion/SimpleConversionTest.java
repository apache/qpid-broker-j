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

package org.apache.qpid.systests.end_to_end_conversion;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.qpid.systests.end_to_end_conversion.client.VerificationException;

public class SimpleConversionTest extends EndToEndConversionTestBase
{
    private static final long TEST_TIMEOUT = 30000L;

    @Test
    public void textMessage() throws Exception
    {
        final JmsInstructions.MessageDescription messageDescription = new JmsInstructions.MessageDescription();
        messageDescription.setMessageType(JmsInstructions.MessageDescription.MessageType.TEXT_MESSAGE);
        messageDescription.setContent("foobar");

        performSimpleTest(messageDescription);
    }

    @Test
    public void bytesMessage() throws Exception
    {
        final JmsInstructions.MessageDescription messageDescription = new JmsInstructions.MessageDescription();
        messageDescription.setMessageType(JmsInstructions.MessageDescription.MessageType.BYTES_MESSAGE);
        messageDescription.setContent(new byte[]{0x00, (byte) 0xFF, (byte) 0xc3});

        performSimpleTest(messageDescription);
    }

    @Test
    public void mapMessage() throws Exception
    {
        final JmsInstructions.MessageDescription messageDescription = new JmsInstructions.MessageDescription();
        messageDescription.setMessageType(JmsInstructions.MessageDescription.MessageType.MAP_MESSAGE);
        HashMap<String, Object> content = new HashMap<>();
        content.put("int", 42);
        content.put("boolean", true);
        content.put("string", "testString");
        messageDescription.setContent(content);

        performSimpleTest(messageDescription);
    }

    @Test
    public void correlationId() throws Exception
    {
        final String correlationId = "myCorrelationId";
        final JmsInstructions.MessageDescription messageDescription = new JmsInstructions.MessageDescription();
        messageDescription.setHeader(JmsInstructions.MessageDescription.MessageHeader.CORRELATION_ID, correlationId);

        performSimpleTest(messageDescription);
    }

    @Ignore("QPID-7897")
    @Test
    public void correlationIdAsBytes() throws Exception
    {
        final byte[] correlationId = new byte[]{(byte) 0xFF, 0x00, (byte) 0xC3};
        final JmsInstructions.MessageDescription messageDescription = new JmsInstructions.MessageDescription();
        messageDescription.setHeader(JmsInstructions.MessageDescription.MessageHeader.CORRELATION_ID, correlationId);

        performSimpleTest(messageDescription);
    }

    @Test
    public void property() throws Exception
    {
        final JmsInstructions.MessageDescription messageDescription = new JmsInstructions.MessageDescription();
        messageDescription.setProperty("intProperty", 42);
        messageDescription.setProperty("stringProperty", "foobar");
        messageDescription.setProperty("booleanProperty", true);
        messageDescription.setProperty("doubleProperty", 37.5);

        performSimpleTest(messageDescription);
    }

    public void performSimpleTest(final JmsInstructions.MessageDescription messageDescription) throws Exception
    {
        final ListenableFuture<?> publisherFuture =
                runPublisher(JmsInstructionBuilder.publishSingleMessage(messageDescription));
        final ListenableFuture<?> subscriberFuture =
                runSubscriber(JmsInstructionBuilder.receiveSingleMessage(messageDescription));
        try
        {
            Futures.allAsList(publisherFuture, subscriberFuture).get(TEST_TIMEOUT, TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException e)
        {
            final Throwable cause = e.getCause();
            if (cause instanceof VerificationException)
            {
                throw new AssertionError("Client failed verification", cause);
            }
            else if (cause instanceof Exception)
            {
                throw ((Exception) cause);
            }
            else
            {
                throw e;
            }
        }
    }
}
