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

import static org.apache.qpid.disttest.client.ParticipantTestHelper.assertExpectedConsumerResults;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.jms.Message;
import javax.jms.Session;

import org.apache.qpid.disttest.jms.ClientJmsDelegate;
import org.apache.qpid.disttest.message.CreateConsumerCommand;
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

public class ConsumerParticipantTest extends UnitTestBase
{
    private static final String SESSION_NAME1 = "SESSION1";
    private static final String PARTICIPANT_NAME1 = "PARTICIPANT_NAME1";
    private static final long RECEIVE_TIMEOUT = 100;
    private static final String CLIENT_NAME = "CLIENT_NAME";
    private static final int PAYLOAD_SIZE_PER_MESSAGE = 1024;
    public static final long MAXIMUM_DURATION = 100l;

    private final Message _mockMessage = mock(Message.class);
    private final CreateConsumerCommand _command = new CreateConsumerCommand();
    private ClientJmsDelegate _delegate;
    private ConsumerParticipant _consumerParticipant;

    /** used to check start/end time of results */
    private long _testStartTime;

    @Before
    public void setUp() throws Exception
    {
        _delegate = mock(ClientJmsDelegate.class);

        _command.setSessionName(SESSION_NAME1);
        _command.setParticipantName(PARTICIPANT_NAME1);
        _command.setSynchronous(true);
        _command.setReceiveTimeout(RECEIVE_TIMEOUT);
        _command.setMaximumDuration(MAXIMUM_DURATION);

        _consumerParticipant = new ConsumerParticipant(_delegate, _command);

        when(_delegate.consumeMessage(PARTICIPANT_NAME1, RECEIVE_TIMEOUT)).thenReturn(_mockMessage);
        when(_delegate.calculatePayloadSizeFrom(_mockMessage)).thenReturn(PAYLOAD_SIZE_PER_MESSAGE);
        when(_delegate.getAcknowledgeMode(SESSION_NAME1)).thenReturn(Session.CLIENT_ACKNOWLEDGE);

        _testStartTime = System.currentTimeMillis();
    }

    @Test
    public void testReceiveMessagesForDurationSync() throws Exception
    {

        _consumerParticipant.startDataCollection();
        final ParticipantResult[] result = new ParticipantResult[1];
        _consumerParticipant.startTest(CLIENT_NAME, new ResultReporter()
        {
            @Override
            public void reportResult(final ParticipantResult theResult)
            {
                result[0] = theResult;
                _consumerParticipant.stopTest();
            }
        });

        assertExpectedConsumerResults(result[0], PARTICIPANT_NAME1, CLIENT_NAME, _testStartTime,
                                      Session.CLIENT_ACKNOWLEDGE, null, null, PAYLOAD_SIZE_PER_MESSAGE, null, MAXIMUM_DURATION);

        verify(_delegate, atLeastOnce()).consumeMessage(PARTICIPANT_NAME1, RECEIVE_TIMEOUT);
        verify(_delegate, atLeastOnce()).calculatePayloadSizeFrom(_mockMessage);
        verify(_delegate, atLeastOnce()).commitOrAcknowledgeMessageIfNecessary(SESSION_NAME1, _mockMessage);
    }

    @Test
    public void testReleaseResources()
    {
        _consumerParticipant.releaseResources();
        verify(_delegate).closeTestConsumer(PARTICIPANT_NAME1);
    }
}
