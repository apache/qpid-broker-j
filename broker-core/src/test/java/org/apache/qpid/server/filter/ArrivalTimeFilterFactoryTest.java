/*
 *
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
package org.apache.qpid.server.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class ArrivalTimeFilterFactoryTest extends UnitTestBase
{
    @Test
    public void testNewInstance()
    {
        final long currentTime = System.currentTimeMillis();
        final int periodInSeconds = 60;
        final MessageFilter filter =
                new ArrivalTimeFilterFactory().newInstance(List.of(String.valueOf(periodInSeconds)));

        final Filterable message = mock(Filterable.class);
        when(message.getArrivalTime()).thenReturn(currentTime - periodInSeconds * 1000 - 1);

        assertFalse(filter.matches(message),
                "Message arrived before '1 minute before filter creation' should not be accepted");

        when(message.getArrivalTime()).thenReturn(currentTime - periodInSeconds  * 1000 / 2);
        assertTrue(filter.matches(message),
                   "Message arrived after '1 minute before filter creation' should be accepted");
        when(message.getArrivalTime()).thenReturn(System.currentTimeMillis());
        assertTrue(filter.matches(message), "Message arrived after filter creation should be accepted");
    }
}