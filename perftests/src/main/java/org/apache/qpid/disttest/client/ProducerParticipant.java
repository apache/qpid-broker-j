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
package org.apache.qpid.disttest.client;

import java.util.Date;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Message;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.qpid.disttest.jms.ClientJmsDelegate;
import org.apache.qpid.disttest.message.CreateProducerCommand;
import org.apache.qpid.disttest.message.ParticipantResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerParticipant implements Participant
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerParticipant.class);

    private final ClientJmsDelegate _jmsDelegate;

    private final CreateProducerCommand _command;

    private final ParticipantResultFactory _resultFactory;

    private final CountDownLatch _startDataCollectionLatch = new CountDownLatch(1);
    private final CountDownLatch _stopTestLatch = new CountDownLatch(1);
    private final CountDownLatch _hasStoppedLatch = new CountDownLatch(1);
    private final long _maximumDuration;
    private final long _numberOfMessages;
    private final int _batchSize;
    private final int _acknowledgeMode;
    private final RateLimiter _rateLimiter;
    private volatile boolean _collectData = false;

    public ProducerParticipant(final ClientJmsDelegate jmsDelegate, final CreateProducerCommand command)
    {
        _jmsDelegate = jmsDelegate;
        _command = command;
        _resultFactory = new ParticipantResultFactory();
        _maximumDuration = _command.getMaximumDuration();
        _numberOfMessages = _command.getNumberOfMessages();
        _batchSize = _command.getBatchSize();
        _acknowledgeMode = _jmsDelegate.getAcknowledgeMode(_command.getSessionName());
        final double rate = _command.getRate();
        _rateLimiter = (rate > 0 ? RateLimiter.create(rate) : null);
    }

    @Override
    public void startTest(String registeredClientName, ResultReporter resultReporter) throws Exception
    {

        long startTime = 0;
        Message lastPublishedMessage = null;
        int numberOfMessagesSent = 0;
        long totalPayloadSizeOfAllMessagesSent = 0;
        NavigableSet<Integer> allProducedPayloadSizes = new TreeSet<>();

        LOGGER.debug("Producer {} about to send messages. Duration limit: {} ms Message Limit : {}",
                    getName(), _maximumDuration, _numberOfMessages);

        while (_stopTestLatch.getCount() != 0)
        {
            if (_rateLimiter != null)
            {
                _rateLimiter.acquire();
            }

            if (_collectData)
            {
                if (startTime == 0)
                {
                    startTime = System.currentTimeMillis();
                }

                if ((_maximumDuration > 0 && System.currentTimeMillis() - startTime >= _maximumDuration) ||
                    (_numberOfMessages > 0 && numberOfMessagesSent >= _numberOfMessages))
                {
                    ParticipantResult result = finaliseResults(registeredClientName,
                                                               startTime,
                                                               numberOfMessagesSent,
                                                               totalPayloadSizeOfAllMessagesSent,
                                                               allProducedPayloadSizes);
                    resultReporter.reportResult(result);
                    _collectData = false;
                }

                lastPublishedMessage = _jmsDelegate.sendNextMessage(_command);

                numberOfMessagesSent++;

                int lastPayloadSize = _jmsDelegate.calculatePayloadSizeFrom(lastPublishedMessage);
                totalPayloadSizeOfAllMessagesSent += lastPayloadSize;
                allProducedPayloadSizes.add(lastPayloadSize);

                LOGGER.trace("message {} sent by {}", numberOfMessagesSent, this);

                final boolean batchLimitReached = (_batchSize <= 0 || numberOfMessagesSent % _batchSize == 0);

                if (batchLimitReached)
                {
                    if (LOGGER.isTraceEnabled() && _batchSize > 0)
                    {
                        LOGGER.trace("Committing: batch size " + _batchSize);
                    }
                    _jmsDelegate.commitIfNecessary(_command.getSessionName());

                    doSleepForInterval();
                }
            }
            else
            {
                if (_maximumDuration > 0)
                {
                    _jmsDelegate.sendNextMessage(_command);

                    _jmsDelegate.commitIfNecessary(_command.getSessionName());
                    LOGGER.trace("Pre-message sent by {}", this);
                }
                if (_rateLimiter == null && _maximumDuration == 0)
                {
                    if (!_startDataCollectionLatch.await(1, TimeUnit.SECONDS))
                    {
                        LOGGER.debug("Producer {} still waiting for collectingData command from coordinator", getName());
                    }
                }
            }
        }
        _hasStoppedLatch.countDown();
    }

    private ParticipantResult finaliseResults(final String registeredClientName,
                                              final long startTime,
                                              final int numberOfMessagesSent,
                                              final long totalPayloadSizeOfAllMessagesSent,
                                              final NavigableSet<Integer> allProducedPayloadSizes)
    {
        // commit the remaining batch messages
        if (_batchSize > 0 && numberOfMessagesSent % _batchSize != 0)
        {
            LOGGER.trace("Committing: batch size {}", _batchSize);
            _jmsDelegate.commitIfNecessary(_command.getSessionName());
        }
        Date end = new Date();

        LOGGER.info("Producer {} finished publishing. Number of messages published: {}",
                    getName(), numberOfMessagesSent);

        Date start = new Date(startTime);
        String providerVersion = _jmsDelegate.getProviderVersion(_command.getSessionName());
        String protocolVersion = _jmsDelegate.getProtocolVersion(_command.getSessionName());
        int payloadSize = getPayloadSizeForResultIfConstantOrZeroOtherwise(allProducedPayloadSizes);

        return _resultFactory.createForProducer(
                getName(),
                registeredClientName,
                _command,
                _acknowledgeMode,
                numberOfMessagesSent,
                payloadSize,
                totalPayloadSizeOfAllMessagesSent,
                start,
                end,
                providerVersion,
                protocolVersion);
    }

    @Override
    public void startDataCollection()
    {
        _collectData = true;
        _startDataCollectionLatch.countDown();
    }

    @Override
    public void stopTest()
    {
        stopTestAsync();
        try
        {
            while (!_hasStoppedLatch.await(1, TimeUnit.SECONDS))
            {
                LOGGER.debug("Producer {} still waiting for shutdown", getName());
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    void stopTestAsync()
    {
        _stopTestLatch.countDown();
    }

    private int getPayloadSizeForResultIfConstantOrZeroOtherwise(NavigableSet<Integer> allPayloadSizes)
    {
        return allPayloadSizes.size() == 1 ? allPayloadSizes.first() : 0;
    }

    private void doSleepForInterval() throws InterruptedException
    {
        long sleepTime = _command.getInterval();
        if (sleepTime > 0)
        {
            doSleep(sleepTime);
        }
    }

    private void doSleep(long sleepTime)
    {
        try
        {
            Thread.sleep(sleepTime);
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void releaseResources()
    {
        _jmsDelegate.closeTestProducer(_command.getParticipantName());
    }

    @Override
    public String getName()
    {
        return _command.getParticipantName();
    }

    @Override
    public String toString()
    {
        return "ProducerParticipant " + getName() + " [command=" + _command + "]";
    }

}
