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

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.disttest.message.ParticipantResult;

public class ParticipantExecutor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ParticipantExecutor.class);

    private final Executor _executor;
    private final Participant _participant;
    private final ParticipantResultFactory _factory;

    private Client _client;

    public ParticipantExecutor(Participant participant, Executor executor)
    {
        _participant = participant;
        _factory = new ParticipantResultFactory();
        _executor = executor;
    }

    /**
     * Schedules the test participant to be run in a background thread.
     */
    public void start(Client client)
    {
        _client = client;

        LOGGER.debug("Starting test participant in background thread: {} ", this);
        _executor.execute(new ParticipantRunnable());
    }

    public String getParticipantName()
    {
        return _participant.getName();
    }

    private class ParticipantRunnable implements Runnable
    {
        @Override
        public final void run()
        {
            Thread currentThread = Thread.currentThread();
            final String initialThreadName = currentThread.getName();
            currentThread.setName(initialThreadName + "-" + getParticipantName());

            try
            {
                runParticipantAndSendResults();
            }
            finally
            {
                currentThread.setName(initialThreadName);
            }
        }

        private void runParticipantAndSendResults()
        {
            ParticipantResult result = null;
            try
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("About to run participant " + _participant);
                }
                result = _participant.doIt(_client.getClientName());
            }
            catch (Exception t)
            {
                String errorMessage = "Unhandled error: " + t.getMessage();
                LOGGER.error(errorMessage, t);
                result = _factory.createForError(_participant.getName(), _client.getClientName(), errorMessage);
            }
            finally
            {
                try
                {
                    _participant.releaseResources();
                }
                catch(Exception e)
                {
                    LOGGER.error("Participant " + _participant + " unable to release resources", e);
                }

                _client.sendResults(result);
            }
        }
    }

    @Override
    public String toString()
    {
        return "ParticipantExecutor[" +
               "participantName=" + _participant.getName() +
               ", client=" + _client +
               ']';
    }
}
