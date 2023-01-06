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
package org.apache.qpid.disttest.controller.config;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.disttest.message.CreateConnectionCommand;
import org.apache.qpid.disttest.message.CreateConsumerCommand;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class IterationValueTest extends UnitTestBase
{
    private static final int MAXIMUM_DURATION = 10;

    private static final boolean IS_DURABLE_SUBSCRIPTION = true;

    private CreateConsumerCommand _createConsumerCommand;
    private Map<String, String> _iterationValueMap;

    @BeforeEach
    public void setUp() throws Exception
    {
        _createConsumerCommand = mock(CreateConsumerCommand.class);

        _iterationValueMap = new HashMap<>();
        _iterationValueMap.put("_maximumDuration", String.valueOf(MAXIMUM_DURATION));
        _iterationValueMap.put("_durableSubscription", String.valueOf(IS_DURABLE_SUBSCRIPTION));
    }

    @Test
    public void testApplyPopulatedIterationValueToCommandWithMatchingProperties()
    {
        IterationValue iterationValue = new IterationValue(_iterationValueMap);

        iterationValue.applyToCommand(_createConsumerCommand);

        verify(_createConsumerCommand).setMaximumDuration(MAXIMUM_DURATION);
        verify(_createConsumerCommand).setDurableSubscription(IS_DURABLE_SUBSCRIPTION);
    }

    @Test
    public void testApplyPopulatedIterationValueToCommandWithoutMatchingProperties()
    {
        IterationValue iterationValue = new IterationValue(_iterationValueMap);

        CreateConnectionCommand createConnectionCommand = mock(CreateConnectionCommand.class);
        iterationValue.applyToCommand(createConnectionCommand);

        verifyNoInteractions(createConnectionCommand);
    }

    @Test
    public void testApplyUnpopulatedIterationValueToCommand()
    {
        IterationValue iterationValue = new IterationValue();

        iterationValue.applyToCommand(_createConsumerCommand);

        verifyNoInteractions(_createConsumerCommand);
    }

}
