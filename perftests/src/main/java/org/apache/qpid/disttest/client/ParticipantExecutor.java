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


public class ParticipantExecutor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ParticipantExecutor.class);

    private final Executor _executor;
    private final Participant _participant;
    private final ParticipantResultFactory _factory;

    private String _clientName;
    private ResultReporter _resultReporter;

    public ParticipantExecutor(Participant participant, Executor executor)
    {
        _participant = participant;
        _factory = new ParticipantResultFactory();
        _executor = executor;
    }

    /**
     * Schedules the test participant to be run in a background thread.
     * @param clientName
     * @param resultReporter
     */
    public void start(String clientName, ResultReporter resultReporter)
    {
        _clientName = clientName;
        _resultReporter = resultReporter;

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
            try
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("About to run participant " + _participant);
                }
                _participant.startTest(_clientName, _resultReporter);
            }
            catch (Exception t)
            {
                String errorMessage = "Unhandled error: " + t.getMessage();
                LOGGER.error(errorMessage, t);
                _resultReporter.reportResult(_factory.createForError(_participant.getName(), _clientName, errorMessage));
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
            }
        }
    }

    @Override
    public String toString()
    {
        return "ParticipantExecutor[" +
               "participantName=" + _participant.getName() +
               ", client=" + _clientName +
               ']';
    }
}
