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

import static org.mockito.Mockito.mock;

import org.junit.Assert;

import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ParticipantRegistryTest extends UnitTestBase
{
    private ParticipantExecutorRegistry _participantRegistry = new ParticipantExecutorRegistry();

    private ParticipantExecutor _testParticipant1 = mock(ParticipantExecutor.class);
    private ParticipantExecutor _testParticipant2 = mock(ParticipantExecutor.class);

    @Test
    public void testAdd()
    {
        assertTrue(_participantRegistry.executors().isEmpty());

        _participantRegistry.add(_testParticipant1);

        assertTrue(_participantRegistry.executors().contains(_testParticipant1));

        _participantRegistry.add(_testParticipant2);

        assertTrue(_participantRegistry.executors().contains(_testParticipant2));
    }

    @Test
    public void testClear()
    {
        _participantRegistry.add(_testParticipant1);

        assertEquals((long) 1, (long) _participantRegistry.executors().size());

        _participantRegistry.clear();

        assertTrue(_participantRegistry.executors().isEmpty());
    }
}
