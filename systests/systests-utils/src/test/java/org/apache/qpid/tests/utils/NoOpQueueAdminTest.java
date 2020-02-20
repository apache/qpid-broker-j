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

package org.apache.qpid.tests.utils;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class NoOpQueueAdminTest extends UnitTestBase
{

    private NoOpQueueAdmin _admin;
    private BrokerAdmin _brokerAdmin;

    @Before
    public void setUp()
    {
        _admin = new NoOpQueueAdmin();
        _brokerAdmin = mock(BrokerAdmin.class);
    }

    @Test
    public void createQueue()
    {
        _admin.createQueue(_brokerAdmin, getTestName());
        verifyNoInteractions(_brokerAdmin);
    }

    @Test
    public void deleteQueue()
    {
        _admin.deleteQueue(_brokerAdmin, getTestName());
        verifyNoInteractions(_brokerAdmin);
    }

    @Test
    public void putMessageOnQueue()
    {
        _admin.putMessageOnQueue(_brokerAdmin, getTestName());
        verifyNoInteractions(_brokerAdmin);
    }

    @Test
    public void isDeleteQueueSupported()
    {
        assertFalse(_admin.isDeleteQueueSupported());
    }

    @Test
    public void isPutMessageOnQueueSupported()
    {
        assertFalse(_admin.isPutMessageOnQueueSupported());
    }
}
