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
package org.apache.qpid.disttest.client;

import static org.apache.qpid.disttest.client.ParticipantTestHelper.assertExpectedProducerResults;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.jms.Message;
import javax.jms.Session;

import org.apache.qpid.disttest.jms.ClientJmsDelegate;
import org.apache.qpid.disttest.message.CreateProducerCommand;
import org.apache.qpid.disttest.message.ParticipantResult;

import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ProducerParticipantTest extends UnitTestBase
{
    public static final int MAXIMUM_DURATION = 1000;
    private ProducerParticipant _producer;

    private static final String SESSION_NAME1 = "SESSION1";
    private static final String PARTICIPANT_NAME1 = "PARTICIPANT_NAME1";

    private static final String CLIENT_NAME = "CLIENT_NAME";
    private static final int PAYLOAD_SIZE_PER_MESSAGE = 1024;


    private final Message _mockMessage = mock(Message.class);
    private final CreateProducerCommand _command = new CreateProducerCommand();
    private ClientJmsDelegate _delegate;

    /** used to check start/end time of results */
    private long _testStartTime;

    @Before
    public void setUp() throws Exception
    {
        _delegate = mock(ClientJmsDelegate.class);

        _command.setSessionName(SESSION_NAME1);
        _command.setParticipantName(PARTICIPANT_NAME1);
        _command.setRate(1000);
        _command.setMaximumDuration((long) MAXIMUM_DURATION);

        when(_delegate.sendNextMessage(isA(CreateProducerCommand.class))).thenReturn(_mockMessage);
        when(_delegate.calculatePayloadSizeFrom(_mockMessage)).thenReturn(PAYLOAD_SIZE_PER_MESSAGE);
        when(_delegate.getAcknowledgeMode(SESSION_NAME1)).thenReturn(Session.AUTO_ACKNOWLEDGE);

        _producer = new ProducerParticipant(_delegate, _command);

        _testStartTime = System.currentTimeMillis();
    }

    @Test
    public void testSendMessagesForDuration() throws Exception
    {
        _producer.startDataCollection();
        final ParticipantResult[] result = new ParticipantResult[1];
        ResultReporter resultReporter = new ResultReporter()
        {
            @Override
            public void reportResult(final ParticipantResult theResult)
            {
                result[0] = theResult;
                _producer.stopTestAsync();
            }
        };
        _producer.startTest(CLIENT_NAME, resultReporter);
        assertExpectedProducerResults(result[0],
                                      PARTICIPANT_NAME1,
                                      CLIENT_NAME,
                                      _testStartTime,
                                      Session.AUTO_ACKNOWLEDGE,
                                      null,
                                      null,
                                      PAYLOAD_SIZE_PER_MESSAGE,
                                      null,
                                      (long) MAXIMUM_DURATION);

        verify(_delegate, atLeastOnce()).sendNextMessage(isA(CreateProducerCommand.class));
        verify(_delegate, atLeastOnce()).calculatePayloadSizeFrom(_mockMessage);
        verify(_delegate, atLeastOnce()).commitIfNecessary(SESSION_NAME1);
    }

    @Test
    public void testReleaseResources()
    {
        _producer.releaseResources();
        verify(_delegate).closeTestProducer(PARTICIPANT_NAME1);
    }
}
