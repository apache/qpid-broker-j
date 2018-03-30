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

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.util.concurrent.Executor;

import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;

import org.apache.qpid.disttest.DistributedTestException;
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

public class ParticipantExecutorTest extends UnitTestBase
{
    private static final ResultHasError HAS_ERROR = new ResultHasError();
    private static final String CLIENT_NAME = "CLIENT_NAME";
    private static final String PARTICIPANT_NAME = "PARTICIPANT_NAME";
    private ParticipantExecutor _participantExecutor = null;
    private Participant _participant = null;
    private ResultReporter _resultReporter;

    @Before
    public void setUp() throws Exception
    {

        _participant = mock(Participant.class);

        _participantExecutor = new ParticipantExecutor(_participant, new SynchronousExecutor());

        _resultReporter = mock(ResultReporter.class);
    }

    @Test
    public void testStart() throws Exception
    {
        _participantExecutor.start(CLIENT_NAME, _resultReporter);
        InOrder inOrder = inOrder(_participant);

        inOrder.verify(_participant).startTest(CLIENT_NAME, _resultReporter);
        inOrder.verify(_participant).releaseResources();
    }

    @Test
    public void testParticipantThrowsException() throws Exception
    {
        doThrow(DistributedTestException.class).when(_participant).startTest(CLIENT_NAME, _resultReporter);
        _participantExecutor.start(CLIENT_NAME, _resultReporter);

        InOrder inOrder = inOrder(_participant, _resultReporter);

        inOrder.verify(_participant).startTest(CLIENT_NAME, _resultReporter);
        inOrder.verify(_resultReporter).reportResult(argThat(HAS_ERROR));
        inOrder.verify(_participant).releaseResources();
    }

    /** avoids our unit test needing to use multiple threads */
    private final class SynchronousExecutor implements Executor
    {
        @Override
        public void execute(Runnable command)
        {
            command.run();
        }
    }

    private static class ResultHasError extends ArgumentMatcher<ParticipantResult>
    {
        @Override
        public boolean matches(Object argument)
        {
            ParticipantResult result = (ParticipantResult) argument;
            return result.hasError();
        }
    }

}
