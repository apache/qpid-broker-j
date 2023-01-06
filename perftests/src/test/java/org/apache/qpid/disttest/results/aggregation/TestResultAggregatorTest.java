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
package org.apache.qpid.disttest.results.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.List;

import org.apache.qpid.disttest.controller.TestResult;
import org.apache.qpid.disttest.message.ConsumerParticipantResult;
import org.apache.qpid.disttest.message.ParticipantResult;
import org.apache.qpid.disttest.message.ProducerParticipantResult;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class TestResultAggregatorTest extends UnitTestBase
{
    private static final String TEST1_NAME = "TEST1_NAME";
    private static final int TEST1_ITERATION_NUMBER = 1;

    private static final String CONSUMER_PARTICIPANT_NAME1 = "CONSUMER_PARTICIPANT_NAME1";
    private static final String CONSUMER_PARTICIPANT_NAME2 = "CONSUMER_PARTICIPANT_NAME2";

    private static final String PRODUCER_PARTICIPANT_NAME = "PRODUCER_PARTICIPANT_NAME";

    private static final String PROVIDER_VERSION = "PROVIDER_VERSION";
    private static final String PROTOCOL_VERSION = "PROTOCOL_VERSION";

    private static final long CONSUMER1_STARTDATE = 50;
    private static final long CONSUMER1_ENDDATE = 20000;

    private static final long CONSUMER2_STARTDATE = 100;
    private static final long CONSUMER2_ENDDATE = 21000;

    private static final long PRODUCER_STARTDATE = 0;
    private static final long PRODUCER_ENDDATE = 19000;

    private static final long NUMBER_OF_MESSAGES_PROCESSED_PER_CONSUMER = 50;
    private static final long NUMBER_OF_MESSAGES_CONSUMED_IN_TOTAL = NUMBER_OF_MESSAGES_PROCESSED_PER_CONSUMER * 2;
    private static final long NUMBER_OF_MESSAGES_PRODUCED = NUMBER_OF_MESSAGES_PROCESSED_PER_CONSUMER * 2;

    private static final int PAYLOAD_SIZE = 1024;
    private static final long TOTAL_PAYLOAD_PROCESSED_PER_CONSUMER = NUMBER_OF_MESSAGES_PROCESSED_PER_CONSUMER * PAYLOAD_SIZE;
    private static final long TOTAL_PAYLOAD_PRODUCED_IN_TOTAL =  TOTAL_PAYLOAD_PROCESSED_PER_CONSUMER * 2;

    private static final int EXPECTED_NUMBER_OF_AGGREGATED_RESULTS = 3;

    private static final int BATCH_SIZE = 3;

    private final TestResultAggregator _aggregator = new TestResultAggregator();

    @Test
    public void testAggregateResultsForTwoConsumerAndOneProducer()
    {
        TestResult originalTestResult = createResultsFromTest();

        int numberOfOriginalParticipantResults = originalTestResult.getParticipantResults().size();
        int expectedNumberOfResults = numberOfOriginalParticipantResults + EXPECTED_NUMBER_OF_AGGREGATED_RESULTS;

        AggregatedTestResult aggregatedTestResult = _aggregator.aggregateTestResult(originalTestResult);

        aggregatedTestResult.getAllConsumerParticipantResult().getTotalPayloadProcessed();
        assertEquals(expectedNumberOfResults, aggregatedTestResult.getParticipantResults().size());

        assertMinimalAggregatedResults(
                aggregatedTestResult.getAllConsumerParticipantResult(),
                TEST1_NAME, TEST1_ITERATION_NUMBER,
                BATCH_SIZE, NUMBER_OF_MESSAGES_CONSUMED_IN_TOTAL, 2, 0,
                PROVIDER_VERSION, PROTOCOL_VERSION);

        assertMinimalAggregatedResults(
                aggregatedTestResult.getAllProducerParticipantResult(),
                TEST1_NAME, TEST1_ITERATION_NUMBER,
                BATCH_SIZE, NUMBER_OF_MESSAGES_PRODUCED, 0, 1,
                PROVIDER_VERSION, PROTOCOL_VERSION);

        assertMinimalAggregatedResults(
                aggregatedTestResult.getAllParticipantResult(),
                TEST1_NAME, TEST1_ITERATION_NUMBER,
                BATCH_SIZE, NUMBER_OF_MESSAGES_CONSUMED_IN_TOTAL, 2, 1,
                PROVIDER_VERSION, PROTOCOL_VERSION);
    }

    @Test
    public void testAggregateResultsWhenParticipantErrored()
    {
        ParticipantResult failedParticipantResult = new ParticipantResult();
        failedParticipantResult.setParticipantName(PRODUCER_PARTICIPANT_NAME);
        failedParticipantResult.setErrorMessage("error");
        TestResult result = new TestResult(TEST1_NAME);
        result.addParticipantResult(failedParticipantResult);

        AggregatedTestResult aggregatedTestResult = _aggregator.aggregateTestResult(result);
        assertEquals(TestResultAggregator.AGGREGATED_ERROR_MESSAGE, aggregatedTestResult.getAllParticipantResult().getErrorMessage());

    }

    @Test
    public void testAggregateResultsForConsumerWithLatencyResults()
    {
        TestResult originalTestResult = createResultsFromTest();
        List<ParticipantResult> results = originalTestResult.getParticipantResults();
        for (ParticipantResult participantResult : results)
        {
            if (participantResult instanceof ConsumerParticipantResult)
            {
                ((ConsumerParticipantResult)participantResult).setMessageLatencies(SeriesStatisticsTest.SERIES);
                break;
            }
        }

        int numberOfOriginalParticipantResults = originalTestResult.getParticipantResults().size();
        int expectedNumberOfResults = numberOfOriginalParticipantResults + EXPECTED_NUMBER_OF_AGGREGATED_RESULTS;

        AggregatedTestResult aggregatedTestResult = _aggregator.aggregateTestResult(originalTestResult);

        aggregatedTestResult.getAllConsumerParticipantResult().getTotalPayloadProcessed();
        assertEquals(expectedNumberOfResults, aggregatedTestResult.getParticipantResults().size());

        assertMinimalAggregatedResults(
                aggregatedTestResult.getAllConsumerParticipantResult(),
                TEST1_NAME, TEST1_ITERATION_NUMBER,
                BATCH_SIZE, NUMBER_OF_MESSAGES_CONSUMED_IN_TOTAL, 2, 0,
                PROVIDER_VERSION, PROTOCOL_VERSION);


        assertLatencyAggregatedResults(aggregatedTestResult.getAllConsumerParticipantResult());

        assertMinimalAggregatedResults(
                aggregatedTestResult.getAllProducerParticipantResult(),
                TEST1_NAME, TEST1_ITERATION_NUMBER,
                BATCH_SIZE, NUMBER_OF_MESSAGES_PRODUCED, 0, 1,
                PROVIDER_VERSION, PROTOCOL_VERSION);


        assertMinimalAggregatedResults(
                aggregatedTestResult.getAllParticipantResult(),
                TEST1_NAME, TEST1_ITERATION_NUMBER,
                BATCH_SIZE, NUMBER_OF_MESSAGES_CONSUMED_IN_TOTAL, 2, 1,
                PROVIDER_VERSION, PROTOCOL_VERSION);


        int expectedThroughtput = (int)Math.round(NUMBER_OF_MESSAGES_PRODUCED * 1000.0d /(CONSUMER2_ENDDATE - PRODUCER_STARTDATE));
        ParticipantResult result = aggregatedTestResult.getAllParticipantResult();
        assertEquals(expectedThroughtput, result.getMessageThroughput(), "Unexpected message throughput");

    }

    private void assertLatencyAggregatedResults(ParticipantResult allConsumerParticipantResult)
    {
        final boolean condition = allConsumerParticipantResult instanceof ConsumerParticipantResult;
        assertTrue(condition, "Unexpected result");
        ConsumerParticipantResult results = (ConsumerParticipantResult)allConsumerParticipantResult;
        assertEquals(5.0, results.getAverageLatency(), 0.01, "Unexpected average");
        assertEquals(2, results.getMinLatency(), "Unexpected min");
        assertEquals(9, results.getMaxLatency(), "Unexpected max");
        assertEquals(2.0, results.getLatencyStandardDeviation(), 0.01, "Unexpected standard deviation");
    }

    private void assertMinimalAggregatedResults(ParticipantResult result, String expectedTestName, int expectedIterationNumber, int expectedBatchSize, long expectedNumberOfMessagesProcessed, int expectedTotalNumberOfConsumers, int expectedTotalNumberOfProducers,
                                                String expectedProviderVersion, String expectedProtocolVersion)
    {
        assertEquals(expectedTestName, result.getTestName(),
                "Unexpected test name in " + result.getParticipantName());

        assertEquals(expectedIterationNumber, result.getIterationNumber(),
                "Unexpected iteration number in " + result.getParticipantName());
        assertEquals(expectedBatchSize, result.getBatchSize(),
                "Unexpected batch size " + result.getParticipantName());
        assertEquals(expectedNumberOfMessagesProcessed, result.getNumberOfMessagesProcessed(),
                "Unexpected number of messages processed in " + result.getParticipantName());
        assertEquals(expectedTotalNumberOfConsumers, result.getTotalNumberOfConsumers(),
                "Unexpected total number of consumers " + result.getParticipantName());
        assertEquals(expectedTotalNumberOfProducers, result.getTotalNumberOfProducers(),
                "Unexpected total number of producers " + result.getParticipantName());
        assertEquals(expectedProviderVersion, result.getProviderVersion(),
                "Unexpected provider version" + result.getProviderVersion());
        assertEquals(expectedProtocolVersion, result.getProtocolVersion(),
                "Unexpected protocol version" + result.getProtocolVersion());
    }

    private TestResult createResultsFromTest()
    {
        TestResult testResult = new TestResult(TEST1_NAME);

        ConsumerParticipantResult consumerResult1 = new ConsumerParticipantResult();
        setPropertiesOn(consumerResult1, TEST1_NAME, TEST1_ITERATION_NUMBER, CONSUMER_PARTICIPANT_NAME1, NUMBER_OF_MESSAGES_PROCESSED_PER_CONSUMER, BATCH_SIZE, PAYLOAD_SIZE, TOTAL_PAYLOAD_PROCESSED_PER_CONSUMER, CONSUMER1_STARTDATE, CONSUMER1_ENDDATE, 1, 0,
                        PROVIDER_VERSION,
                        PROTOCOL_VERSION);
        testResult.addParticipantResult(consumerResult1);

        ConsumerParticipantResult consumerResult2 = new ConsumerParticipantResult();
        setPropertiesOn(consumerResult2, TEST1_NAME, TEST1_ITERATION_NUMBER, CONSUMER_PARTICIPANT_NAME2, NUMBER_OF_MESSAGES_PROCESSED_PER_CONSUMER, BATCH_SIZE, PAYLOAD_SIZE, TOTAL_PAYLOAD_PROCESSED_PER_CONSUMER, CONSUMER2_STARTDATE, CONSUMER2_ENDDATE, 1, 0,
                        PROVIDER_VERSION,
                        PROTOCOL_VERSION);
        testResult.addParticipantResult(consumerResult2);

        ParticipantResult producerResult = new ProducerParticipantResult();
        setPropertiesOn(producerResult, TEST1_NAME, TEST1_ITERATION_NUMBER, PRODUCER_PARTICIPANT_NAME, NUMBER_OF_MESSAGES_PRODUCED, BATCH_SIZE, PAYLOAD_SIZE, TOTAL_PAYLOAD_PRODUCED_IN_TOTAL, PRODUCER_STARTDATE, PRODUCER_ENDDATE, 0, 1,
                        PROVIDER_VERSION,
                        PROTOCOL_VERSION);
        testResult.addParticipantResult(producerResult);

        return testResult;
    }

    private void setPropertiesOn(ParticipantResult participantResult,
                                 String testName,
                                 int iterationNumber,
                                 String participantName,
                                 long numberOfMessagesProcessed,
                                 int batchSize,
                                 int payloadSize,
                                 long totalPayloadProcessed,
                                 long start,
                                 long end,
                                 int totalNumberOfConsumers,
                                 int totalNumberOfProducers,
                                 String providerVersion,
                                 String protocolVersion)
    {
        participantResult.setParticipantName(participantName);
        participantResult.setTestName(testName);
        participantResult.setIterationNumber(iterationNumber);
        participantResult.setTotalNumberOfConsumers(totalNumberOfConsumers);
        participantResult.setTotalNumberOfProducers(totalNumberOfProducers);

        participantResult.setNumberOfMessagesProcessed(numberOfMessagesProcessed);
        participantResult.setPayloadSize(payloadSize);
        participantResult.setTotalPayloadProcessed(totalPayloadProcessed);
        participantResult.setStartDate(new Date(start));
        participantResult.setEndDate(new Date(end));
        participantResult.setBatchSize(batchSize);

        participantResult.setProviderVersion(providerVersion);
        participantResult.setProtocolVersion(protocolVersion);
    }
}
