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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.qpid.disttest.message.ParticipantResult;

public class ParticipantTestHelper
{

    public static void assertAtLeast(String message, final long minimumExpected, final long actual)
    {
        assertTrue(actual >= minimumExpected, message + " " + actual);
    }

    public static void assertExpectedConsumerResults(ParticipantResult result, String participantName, String registeredClientName, long expectedTestStartTime, int expectedAcknowledgeMode, Integer expectedBatchSize, Integer expectedNumberOfMessages, Integer expectedPayloadSize, Long expectedTotalPayloadProcessed, Long expectedMinimumExpectedDuration)
    {
        assertExpectedResults(result, participantName, registeredClientName, expectedTestStartTime,
                expectedAcknowledgeMode, expectedBatchSize, expectedNumberOfMessages, expectedPayloadSize, expectedTotalPayloadProcessed, expectedMinimumExpectedDuration);
        assertEquals(1, result.getTotalNumberOfConsumers(), "Unexpected number of consumers");
        assertEquals(0, result.getTotalNumberOfProducers(), "Unexpected number of producers");
    }

    public static void assertExpectedProducerResults(ParticipantResult result, String participantName, String registeredClientName, long expectedTestStartTime, int expectedAcknowledgeMode, Integer expectedBatchSize, Integer expectedNumberOfMessages, Integer expectedPayloadSize, Long expectedTotalPayloadProcessed, Long expectedMinimumExpectedDuration)
    {
        assertExpectedResults(result, participantName, registeredClientName, expectedTestStartTime, expectedAcknowledgeMode, expectedBatchSize, expectedNumberOfMessages, expectedPayloadSize, expectedTotalPayloadProcessed, expectedMinimumExpectedDuration);
        assertEquals(1, result.getTotalNumberOfProducers(), "Unexpected number of producers");
        assertEquals(0, result.getTotalNumberOfConsumers(), "Unexpected number of consumers");
    }

    private static void assertExpectedResults(ParticipantResult result, String participantName, String registeredClientName, long expectedTestStartTime, int expectedAcknowledgeMode, Integer expectedBatchSize, Integer expectedNumberOfMessages, Integer expectedPayloadSize, Long expectedTotalPayloadProcessed, Long expectedMinimumExpectedDuration)
    {
        assertFalse(result.hasError());

        assertEquals(participantName, result.getParticipantName(), "unexpected participant name");
        assertEquals(registeredClientName, result.getRegisteredClientName(), "unexpected client name");

        assertAtLeast("start time of result is too low", expectedTestStartTime, result.getStartInMillis());
        assertAtLeast("end time of result should be after start time", result.getStartInMillis(), result.getEndInMillis());

        assertEquals(expectedAcknowledgeMode, result.getAcknowledgeMode(), "unexpected acknowledge mode");

        if(expectedNumberOfMessages != null)
        {
            assertEquals(expectedNumberOfMessages.intValue(), result.getNumberOfMessagesProcessed(), "unexpected number of messages");
        }
        if(expectedBatchSize != null)
        {
            assertEquals(expectedBatchSize.intValue(), result.getBatchSize(), "unexpected batch size");
        }
        if (expectedPayloadSize != null)
        {
            assertEquals(expectedPayloadSize.intValue(), result.getPayloadSize(), "unexpected payload size");
        }
        if (expectedTotalPayloadProcessed != null)
        {
            assertEquals(expectedTotalPayloadProcessed.longValue(), result.getTotalPayloadProcessed(), "unexpected total payload processed");
        }
        if(expectedMinimumExpectedDuration != null)
        {
            assertAtLeast("participant did not take a sufficient length of time.", expectedMinimumExpectedDuration, result.getTimeTaken());
        }
    }
}
