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


import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.disttest.jms.ClientJmsDelegate;
import org.apache.qpid.disttest.message.CreateConsumerCommand;
import org.apache.qpid.disttest.message.ParticipantResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerParticipant implements Participant
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerParticipant.class);

    private final AtomicInteger _totalNumberOfMessagesReceived = new AtomicInteger(0);
    private final NavigableSet<Integer> _allConsumedPayloadSizes = new ConcurrentSkipListSet<Integer>();
    private final AtomicLong _totalPayloadSizeOfAllMessagesReceived = new AtomicLong(0);
    private final CountDownLatch _asyncRunHasFinished = new CountDownLatch(1);
    private final ClientJmsDelegate _jmsDelegate;
    private final CreateConsumerCommand _command;
    private final ParticipantResultFactory _resultFactory;
    private final AtomicBoolean _collectingData = new AtomicBoolean();
    private final boolean _batchEnabled;
    private final long _expectedNumberOfMessages;
    private final boolean _evaluateLatency;
    private final int _batchSize;
    private final long _maximumDuration;

    private volatile long _startTime;
    private volatile Exception _asyncMessageListenerException;
    private List<Long> _messageLatencies;
    private final long _syncReceiveTimeout;

    public ConsumerParticipant(final ClientJmsDelegate delegate, final CreateConsumerCommand command)
    {
        _jmsDelegate = delegate;
        _command = command;
        _syncReceiveTimeout = _command.getReceiveTimeout() > 0 ? _command.getReceiveTimeout() : 50l;
        _batchSize = _command.getBatchSize();
        _batchEnabled = _batchSize > 0;


        _resultFactory = new ParticipantResultFactory();
        if (command.isEvaluateLatency())
        {
            _messageLatencies = new ArrayList<>();
        }
        _expectedNumberOfMessages = _command.getNumberOfMessages();
        _evaluateLatency = _command.isEvaluateLatency();
        _maximumDuration = _command.getMaximumDuration();
    }

    @Override
    public void startTest(String registeredClientName, ResultReporter resultReporter) throws Exception
    {
        final int acknowledgeMode = _jmsDelegate.getAcknowledgeMode(_command.getSessionName());
        final String providerVersion = _jmsDelegate.getProviderVersion(_command.getSessionName());
        final String protocolVersion = _jmsDelegate.getProtocolVersion(_command.getSessionName());

        if (_command.isSynchronous())
        {
            synchronousRun();
        }
        else
        {
            LOGGER.debug("Consumer {} registering listener", getName());

            _jmsDelegate.registerListener(_command.getParticipantName(), new MessageListener(){

                @Override
                public void onMessage(Message message)
                {
                    processAsyncMessage(message);
                }

            });

            waitUntilMsgListenerHasFinished();
            rethrowAnyAsyncMessageListenerException();
        }

        Date end = new Date();
        final Date start = new Date(_startTime);
        int numberOfMessagesReceived = _totalNumberOfMessagesReceived.get();
        long totalPayloadSize = _totalPayloadSizeOfAllMessagesReceived.get();
        int payloadSize = getPayloadSizeForResultIfConstantOrZeroOtherwise(_allConsumedPayloadSizes);

        LOGGER.info("Consumer {} finished consuming. Number of messages consumed: {}",
                    getName(), numberOfMessagesReceived);

        ParticipantResult result = _resultFactory.createForConsumer(
                getName(),
                registeredClientName,
                _command,
                acknowledgeMode,
                numberOfMessagesReceived,
                payloadSize,
                totalPayloadSize,
                start,
                end,
                _messageLatencies,
                providerVersion,
                protocolVersion);
        resultReporter.reportResult(result);
    }

    @Override
    public void startDataCollection()
    {
        _collectingData.set(true);
    }

    @Override
    public void stopTest()
    {
        // noop
    }

    private void synchronousRun()
    {
        LOGGER.debug("Consumer {} about to consume messages", getName());

        boolean keepLooping = true;
        do
        {
            Message message = _jmsDelegate.consumeMessage(_command.getParticipantName(), _syncReceiveTimeout);
            if (message != null)
            {
                keepLooping = processMessage(message);
            }
            else
            {
                boolean reachedMaximumDuration = _startTime > 0 && _maximumDuration > 0 && System.currentTimeMillis() - _startTime >= _maximumDuration;
                if (reachedMaximumDuration)
                {
                    LOGGER.debug("Consumer participant done due to maximumDuration reached.");
                    keepLooping = false;
                }
            }
        }
        while (keepLooping);
    }

    /**
     * @return whether to continue running (ie returns false if the message quota has been reached)
     */
    private boolean processMessage(Message message)
    {
        if (!_collectingData.get() && _expectedNumberOfMessages == 0)
        {
            _jmsDelegate.commitOrAcknowledgeMessageIfNecessary(_command.getSessionName(), message);
            return true;
        }

        if (_startTime == 0)
        {
            _startTime = System.currentTimeMillis();
        }

        LOGGER.trace("Message {} received by {}", message, this);

        final int messageCount = _totalNumberOfMessagesReceived.incrementAndGet() ;

        int messagePayloadSize = _jmsDelegate.calculatePayloadSizeFrom(message);
        _allConsumedPayloadSizes.add(messagePayloadSize);
        _totalPayloadSizeOfAllMessagesReceived.addAndGet(messagePayloadSize);

        if (_evaluateLatency)
        {
            long messageTimestamp = getMessageTimestamp(message);
            long latency = System.currentTimeMillis() - messageTimestamp;
            _messageLatencies.add(latency);
        }

        boolean batchComplete = (_batchEnabled && (messageCount % _batchSize == 0));
        if (!_batchEnabled || batchComplete)
        {
            if (_batchEnabled)
            {
                LOGGER.trace("Committing: batch size {} ", _batchSize);
            }
            _jmsDelegate.commitOrAcknowledgeMessageIfNecessary(_command.getSessionName(), message);
        }

        boolean reachedMaximumDuration = _maximumDuration > 0 && System.currentTimeMillis() - _startTime >= _maximumDuration;
        boolean receivedAllMessages = _expectedNumberOfMessages > 0  && messageCount >= _expectedNumberOfMessages;

        if (reachedMaximumDuration || receivedAllMessages)
        {
            LOGGER.debug("Message {} reached duration : {} : received all messages : {}",
                         messageCount, reachedMaximumDuration, reachedMaximumDuration);

            if (_batchEnabled)
            {
                LOGGER.trace("Committing: final batch");
                // commit/acknowledge remaining messages if necessary
                _jmsDelegate.commitOrAcknowledgeMessageIfNecessary(_command.getSessionName(), message);
            }
            return false;
        }

        return true;
    }

    private long getMessageTimestamp(final Message message)
    {
        try
        {
            return message.getJMSTimestamp();
        }
        catch (JMSException e)
        {
            throw new DistributedTestException("Cannot get message timestamp!", e);
        }
    }


    /**
     * Intended to be called from a {@link MessageListener}. Updates {@link #_asyncRunHasFinished} if
     * no more messages should be processed, causing {@link Participant#startTest(String, ResultReporter)} to exit.
     */
    public void processAsyncMessage(Message message)
    {
        boolean continueRunning = true;
        try
        {
            continueRunning = processMessage(message);
        }
        catch (Exception e)
        {
            LOGGER.error("Error occurred consuming message " + _totalNumberOfMessagesReceived, e);
            continueRunning = false;
            _asyncMessageListenerException = e;
        }

        if(!continueRunning)
        {
            _asyncRunHasFinished.countDown();
        }
    }

    @Override
    public void releaseResources()
    {
        _jmsDelegate.closeTestConsumer(_command.getParticipantName());
    }

    private int getPayloadSizeForResultIfConstantOrZeroOtherwise(NavigableSet<Integer> allSizes)
    {
        return allSizes.size() == 1 ? _allConsumedPayloadSizes.first() : 0;
    }

    private void rethrowAnyAsyncMessageListenerException()
    {
        if (_asyncMessageListenerException != null)
        {
            throw new DistributedTestException(_asyncMessageListenerException);
        }
    }

    private void waitUntilMsgListenerHasFinished() throws Exception
    {
        LOGGER.debug("waiting until message listener has finished for " + this);
        _asyncRunHasFinished.await();
        LOGGER.debug("Message listener has finished for " + this);
    }

    @Override
    public String getName()
    {
        return _command.getParticipantName();
    }

    @Override
    public String toString()
    {
        return "ConsumerParticipant " + getName() + " [_command=" + _command + ", _startTime=" + _startTime + "]";
    }
}
