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
package org.apache.qpid.disttest.message;

import static org.apache.qpid.disttest.message.ParticipantAttribute.BATCH_SIZE;
import static org.apache.qpid.disttest.message.ParticipantAttribute.CONFIGURED_CLIENT_NAME;
import static org.apache.qpid.disttest.message.ParticipantAttribute.ITERATION_NUMBER;
import static org.apache.qpid.disttest.message.ParticipantAttribute.MAXIMUM_DURATION;
import static org.apache.qpid.disttest.message.ParticipantAttribute.MESSAGE_THROUGHPUT;
import static org.apache.qpid.disttest.message.ParticipantAttribute.NUMBER_OF_MESSAGES_PROCESSED;
import static org.apache.qpid.disttest.message.ParticipantAttribute.PARTICIPANT_NAME;
import static org.apache.qpid.disttest.message.ParticipantAttribute.PAYLOAD_SIZE;
import static org.apache.qpid.disttest.message.ParticipantAttribute.TEST_NAME;
import static org.apache.qpid.disttest.message.ParticipantAttribute.THROUGHPUT;

import java.util.Comparator;
import java.util.Date;
import java.util.Map;

public class ParticipantResult extends Response
{
    private String _testName;
    private String _participantName;
    private int _iterationNumber;

    private long _startInMillis;
    private long _endInMillis;
    private int _batchSize;
    private long _maximumDuration;

    private String _configuredClientName;

    private long _numberOfMessagesProcessed;
    private long _totalPayloadProcessed;
    private int _payloadSize;
    private double _throughput;
    private int _messageThroughput;

    private int _totalNumberOfConsumers;
    private int _totalNumberOfProducers;

    private String _providerVersion;
    private String _protocolVersion;

    // As Session.SESSION_TRANSACTED is 0, we use value -1 so we can distinguish the case where an aggregated result
    // summarizes results from participants using different session acknowledge modes.
    private int _acknowledgeMode = -1;

    public static final Comparator<? super ParticipantResult> PARTICIPANT_NAME_COMPARATOR =
            Comparator.comparing(ParticipantResult::getParticipantName);

    public ParticipantResult()
    {
        this(CommandType.PARTICIPANT_RESULT);
    }

    public ParticipantResult(CommandType commandType)
    {
        super(commandType);
    }

    public ParticipantResult(String participantName)
    {
        this();
        setParticipantName(participantName);
    }

    @OutputAttribute(attribute=TEST_NAME)
    public String getTestName()
    {
        return _testName;
    }

    public void setTestName(String testName)
    {
        _testName = testName;
    }

    @OutputAttribute(attribute=ITERATION_NUMBER)
    public int getIterationNumber()
    {
        return _iterationNumber;
    }

    public void setIterationNumber(int iterationNumber)
    {
        _iterationNumber = iterationNumber;
    }

    public void setStartDate(Date start)
    {
        _startInMillis = start.getTime();
    }

    public void setEndDate(Date end)
    {
        _endInMillis = end.getTime();
    }

    public Date getStartDate()
    {
        return new Date(_startInMillis);
    }

    public Date getEndDate()
    {
        return new Date(_endInMillis);
    }


    public long getStartInMillis()
    {
        return _startInMillis;
    }

    public long getEndInMillis()
    {
        return _endInMillis;
    }


    @OutputAttribute(attribute=PARTICIPANT_NAME)
    public String getParticipantName()
    {
        return _participantName;
    }


    public void setParticipantName(String participantName)
    {
        _participantName = participantName;
    }

    @OutputAttribute(attribute=ParticipantAttribute.TIME_TAKEN)
    public long getTimeTaken()
    {
        return _endInMillis - _startInMillis;
    }

    @OutputAttribute(attribute=CONFIGURED_CLIENT_NAME)
    public String getConfiguredClientName()
    {
        return _configuredClientName;
    }

    public void setConfiguredClientName(String configuredClientName)
    {
        _configuredClientName = configuredClientName;
    }

    @OutputAttribute(attribute=NUMBER_OF_MESSAGES_PROCESSED)
    public long getNumberOfMessagesProcessed()
    {
        return _numberOfMessagesProcessed;
    }

    public void setNumberOfMessagesProcessed(long numberOfMessagesProcessed)
    {
        _numberOfMessagesProcessed = numberOfMessagesProcessed;
    }

    @OutputAttribute(attribute=ParticipantAttribute.TOTAL_PAYLOAD_PROCESSED)
    public long getTotalPayloadProcessed()
    {
        return _totalPayloadProcessed;
    }

    @OutputAttribute(attribute = PAYLOAD_SIZE)
    public int getPayloadSize()
    {
        return _payloadSize;
    }

    public void setPayloadSize(int payloadSize)
    {
        _payloadSize = payloadSize;
    }

    public void setTotalPayloadProcessed(long totalPayloadProcessed)
    {
        _totalPayloadProcessed = totalPayloadProcessed;
    }

    public Map<ParticipantAttribute, Object> getAttributes()
    {
        return ParticipantAttributeExtractor.getAttributes(this);
    }

    public void setBatchSize(int batchSize)
    {
        _batchSize = batchSize;
    }

    @OutputAttribute(attribute=BATCH_SIZE)
    public int getBatchSize()
    {
        return _batchSize;
    }

    public void setMaximumDuration(long maximumDuration)
    {
        _maximumDuration = maximumDuration;
    }

    @OutputAttribute(attribute=MAXIMUM_DURATION)
    public long getMaximumDuration()
    {
        return _maximumDuration;
    }

    @OutputAttribute(attribute=THROUGHPUT)
    public double getThroughput()
    {
        return _throughput;
    }

    public void setThroughput(double throughput)
    {
        _throughput = throughput;
    }

    @OutputAttribute(attribute=MESSAGE_THROUGHPUT)
    public int getMessageThroughput()
    {
        return _messageThroughput;
    }

    public void setMessageThroughput(int throughput)
    {
        _messageThroughput = throughput;
    }

    public void setTotalNumberOfConsumers(int totalNumberOfConsumers)
    {
        _totalNumberOfConsumers = totalNumberOfConsumers;
    }

    @OutputAttribute(attribute=ParticipantAttribute.TOTAL_NUMBER_OF_CONSUMERS)
    public int getTotalNumberOfConsumers()
    {
        return _totalNumberOfConsumers;
    }

    public void setTotalNumberOfProducers(int totalNumberOfProducers)
    {
        _totalNumberOfProducers = totalNumberOfProducers;
    }

    @OutputAttribute(attribute=ParticipantAttribute.TOTAL_NUMBER_OF_PRODUCERS)
    public int getTotalNumberOfProducers()
    {
        return _totalNumberOfProducers;
    }

    @OutputAttribute(attribute=ParticipantAttribute.ACKNOWLEDGE_MODE)
    public int getAcknowledgeMode()
    {
        return _acknowledgeMode;
    }

    public void setAcknowledgeMode(int acknowledgeMode)
    {
        _acknowledgeMode = acknowledgeMode;
    }

    public double getLatencyStandardDeviation()
    {
        return 0.0;
    }

    @OutputAttribute(attribute = ParticipantAttribute.MIN_LATENCY)
    public long getMinLatency()
    {
        return 0;
    }

    @OutputAttribute(attribute = ParticipantAttribute.MAX_LATENCY)
    public long getMaxLatency()
    {
        return 0;
    }

    @OutputAttribute(attribute = ParticipantAttribute.AVERAGE_LATENCY)
    public double getAverageLatency()
    {
        return 0;
    }

    public int getPriority()
    {
        return 0;
    }

    public long getTimeToLive()
    {
        return 0;
    }

    public int getDeliveryMode()
    {
        return 0;
    }

    @OutputAttribute(attribute = ParticipantAttribute.PROVIDER_VERSION)
    public String getProviderVersion()
    {
        return _providerVersion;
    }

    public void setProviderVersion(final String providerVersion)
    {
        _providerVersion = providerVersion;
    }

    @OutputAttribute(attribute = ParticipantAttribute.PROTOCOL_VERSION)
    public String getProtocolVersion()
    {
        return _protocolVersion;
    }

    public void setProtocolVersion(final String protocolVersion)
    {
        _protocolVersion = protocolVersion;
    }

}
