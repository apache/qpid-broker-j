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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class ConfigTest extends UnitTestBase
{
    @Test
    public void testGetTestsForTestWithIteratingMessageSizes()
    {
        Config config = createConfigWithIteratingMessageSizes();
        List<TestInstance> testConfigs = config.getTests();

        assertEquals(2, (long) testConfigs.size(), "should have a test config for each message size");

        TestInstance instance0 = testConfigs.get(0);
        assertEquals(0, (long) instance0.getIterationNumber());

        TestInstance instance1 = testConfigs.get(1);
        assertEquals(1, (long) instance1.getIterationNumber());
    }

    private Config createConfigWithIteratingMessageSizes()
    {
        TestConfig testConfig = mock(TestConfig.class);

        when(testConfig.getIterationValues()).thenReturn(Arrays.asList(new IterationValue(),new IterationValue()));

        Config config = new Config(testConfig);

        return config;
    }

}
