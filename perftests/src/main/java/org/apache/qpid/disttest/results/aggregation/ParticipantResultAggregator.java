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
package org.apache.qpid.disttest.results.aggregation;

import java.util.Date;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.qpid.disttest.message.ConsumerParticipantResult;
import org.apache.qpid.disttest.message.ParticipantResult;
import org.apache.qpid.disttest.message.ProducerParticipantResult;

public class ParticipantResultAggregator
{
    private final String _aggregatedResultName;
    private final Class<? extends ParticipantResult> _targetClass;

    private final NavigableSet<Integer> _encounteredPayloadSizes = new TreeSet<>();
    private final NavigableSet<Integer> _encounteredIterationNumbers = new TreeSet<>();
    private final NavigableSet<Integer> _encounteredBatchSizes = new TreeSet<>();
    private final NavigableSet<Integer> _encounteredAcknowledgeMode = new TreeSet<>();
    private final NavigableSet<Integer> _encounteredDeliveryModes = new TreeSet<>();
    private final NavigableSet<Boolean> _encounteredDurableSubscriptions = new TreeSet<>();
    private final NavigableSet<Boolean> _encounteredTopics = new TreeSet<>();
    private final NavigableSet<String> _encounteredTestNames = new TreeSet<>();
    private final NavigableSet<String> _encounteredProviderVersions = new TreeSet<>();
    private final NavigableSet<String> _encounteredProtocolVersions = new TreeSet<>();

    private final SeriesStatistics _latencyStatistics = new SeriesStatistics();

    private long _minStartDate = Long.MAX_VALUE;
    private long _maxEndDate = 0;
    private long _numberOfMessagesProcessed = 0;
    private long _totalPayloadProcessed = 0;

    private int _totalNumberOfConsumers = 0;
    private int _totalNumberOfProducers = 0;

    public ParticipantResultAggregator(Class<? extends ParticipantResult> targetClass, String aggregateResultName)
    {
        _aggregatedResultName = aggregateResultName;
        _targetClass = targetClass;
    }

    public void aggregate(ParticipantResult result)
    {
        if (isAggregatable(result))
        {
            rollupConstantAttributes(result);
            computeVariableAttributes(result);
            if (result instanceof ConsumerParticipantResult)
            {
                ConsumerParticipantResult consumerParticipantResult = (ConsumerParticipantResult)result;
                _latencyStatistics.addMessageLatencies(consumerParticipantResult.getMessageLatencies());
                _latencyStatistics.aggregate();
            }
        }
    }

    public ParticipantResult getAggregatedResult()
    {
        ParticipantResult aggregatedResult;
        if (_targetClass == ConsumerParticipantResult.class)
        {
            ConsumerParticipantResult consumerParticipantResult = new ConsumerParticipantResult(_aggregatedResultName);
            consumerParticipantResult.setAverageLatency(_latencyStatistics.getAverage());
            consumerParticipantResult.setMinLatency(_latencyStatistics.getMinimum());
            consumerParticipantResult.setMaxLatency(_latencyStatistics.getMaximum());
            consumerParticipantResult.setLatencyStandardDeviation(_latencyStatistics.getStandardDeviation());
            aggregatedResult = consumerParticipantResult;
        }
        else
        {
            aggregatedResult = new ParticipantResult(_aggregatedResultName);
        }

        setRolledUpConstantAttributes(aggregatedResult);
        setComputedVariableAttributes(aggregatedResult);

        return aggregatedResult;
    }

    private boolean isAggregatable(ParticipantResult result)
    {
        return _targetClass.isAssignableFrom(result.getClass());
    }

    private void computeVariableAttributes(ParticipantResult result)
    {
        _numberOfMessagesProcessed += result.getNumberOfMessagesProcessed();
        _totalPayloadProcessed += result.getTotalPayloadProcessed();
        _totalNumberOfConsumers += result.getTotalNumberOfConsumers();
        _totalNumberOfProducers += result.getTotalNumberOfProducers();
        _minStartDate = Math.min(_minStartDate, result.getStartInMillis());
        _maxEndDate = Math.max(_maxEndDate, result.getEndInMillis());
    }

    private void rollupConstantAttributes(ParticipantResult result)
    {
        if (result.getTestName() != null)
        {
            _encounteredTestNames.add(result.getTestName());
        }
        if (result.getProviderVersion() != null)
        {
            _encounteredProviderVersions.add(result.getProviderVersion());
        }
        if (result.getProtocolVersion() != null)
        {
            _encounteredProtocolVersions.add(result.getProtocolVersion());
        }
        _encounteredPayloadSizes.add(result.getPayloadSize());
        _encounteredIterationNumbers.add(result.getIterationNumber());
        _encounteredBatchSizes.add(result.getBatchSize());
        _encounteredAcknowledgeMode.add(result.getAcknowledgeMode());
        if (result instanceof ProducerParticipantResult)
        {
            ProducerParticipantResult producerParticipantResult = (ProducerParticipantResult) result;
            _encounteredDeliveryModes.add(producerParticipantResult.getDeliveryMode());
        }
        else if(result instanceof ConsumerParticipantResult)
        {
            ConsumerParticipantResult consumerParticipantResult = (ConsumerParticipantResult)result;
            _encounteredDurableSubscriptions.add(consumerParticipantResult.isDurableSubscription());
            _encounteredTopics.add(consumerParticipantResult.isTopic());
        }
    }

    private void setComputedVariableAttributes(ParticipantResult aggregatedResult)
    {
        aggregatedResult.setNumberOfMessagesProcessed(_numberOfMessagesProcessed);
        aggregatedResult.setTotalPayloadProcessed(_totalPayloadProcessed);
        aggregatedResult.setTotalNumberOfConsumers(_totalNumberOfConsumers);
        aggregatedResult.setTotalNumberOfProducers(_totalNumberOfProducers);
        aggregatedResult.setStartDate(new Date(_minStartDate));
        aggregatedResult.setEndDate(new Date(_maxEndDate));
        aggregatedResult.setThroughput(calculateThroughputInKiloBytesPerSecond());
        aggregatedResult.setMessageThroughput(calculateThroughputInMessagesPerSecond());
    }

    private void setRolledUpConstantAttributes(ParticipantResult aggregatedResult)
    {
        if (_encounteredIterationNumbers.size() == 1)
        {
            aggregatedResult.setIterationNumber( _encounteredIterationNumbers.first());
        }
        if (_encounteredPayloadSizes.size() == 1)
        {
            aggregatedResult.setPayloadSize(_encounteredPayloadSizes.first());
        }
        if (_encounteredTestNames.size() == 1)
        {
            aggregatedResult.setTestName(_encounteredTestNames.first());
        }
        if (_encounteredProviderVersions.size() == 1)
        {
            aggregatedResult.setProviderVersion(_encounteredProviderVersions.first());
        }
        if (_encounteredProtocolVersions.size() == 1)
        {
            aggregatedResult.setProtocolVersion(_encounteredProtocolVersions.first());
        }
        if (_encounteredBatchSizes.size() == 1)
        {
            aggregatedResult.setBatchSize(_encounteredBatchSizes.first());
        }
        if (_encounteredAcknowledgeMode.size() == 1)
        {
            aggregatedResult.setAcknowledgeMode(_encounteredAcknowledgeMode.first());
        }
        if (aggregatedResult instanceof ProducerParticipantResult)
        {
            ProducerParticipantResult producerParticipantResult = (ProducerParticipantResult) aggregatedResult;
            if(_encounteredDeliveryModes.size() == 1)
            {
                producerParticipantResult.setDeliveryMode(_encounteredDeliveryModes.first());
            }
        }
        if (aggregatedResult instanceof ConsumerParticipantResult)
        {
            ConsumerParticipantResult consumerParticipantResult = (ConsumerParticipantResult) aggregatedResult;
            if(_encounteredDurableSubscriptions.size() == 1)
            {
                consumerParticipantResult.setDurableSubscription(_encounteredDurableSubscriptions.first());
            }
            if(_encounteredTopics.size() == 1)
            {
                consumerParticipantResult.setTopic(_encounteredTopics.first());
            }
        }
    }

    private double calculateThroughputInKiloBytesPerSecond()
    {
        double durationInMillis = _maxEndDate - _minStartDate;
        double durationInSeconds = durationInMillis / 1000;
        double totalPayloadProcessedInKiloBytes = ((double)_totalPayloadProcessed) / 1024;

        return totalPayloadProcessedInKiloBytes/durationInSeconds;
    }

    private int calculateThroughputInMessagesPerSecond()
    {
        double durationInMillis = _maxEndDate - _minStartDate;
        if (durationInMillis == 0 )
        {
            return 0;
        }

        return (int)Math.round((_numberOfMessagesProcessed * 1000.0d)/durationInMillis);
    }
}
