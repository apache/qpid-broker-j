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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class ExternalQpidBrokerAdminImplTest extends UnitTestBase
{
    private ExternalQpidBrokerAdminImpl _admin;
    private QueueAdmin _queueAdmin;

    @Before
    public void setUp()
    {
        _queueAdmin = mock(QueueAdmin.class);
        _admin = new ExternalQpidBrokerAdminImpl(_queueAdmin);
    }

    @Test
    public void createQueue()
    {
        final String queueName = getTestName();
        _admin.createQueue(queueName);
        verify(_queueAdmin).createQueue(_admin, queueName);
    }


    @Test
    public void deleteQueue()
    {
        final String queueName = getTestName();
        _admin.createQueue(queueName);
        _admin.deleteQueue(queueName);
        verify(_queueAdmin).deleteQueue(_admin, queueName);
    }

    @Test
    public void putMessageOnQueue()
    {
        final String queueName = getTestName();
        final String testMessage = "Test Message";
        _admin.putMessageOnQueue(queueName, testMessage);
        verify(_queueAdmin).putMessageOnQueue(_admin, queueName, testMessage);
    }

    @Test
    public void isPutMessageOnQueueSupported()
    {
        assertFalse(_admin.isPutMessageOnQueueSupported());
    }

    @Test
    public void isDeleteQueueSupported()
    {
        assertFalse(_admin.isDeleteQueueSupported());
    }

    @Test
    public void afterTestMethod()
    {
        final String queueName1 = getTestName();
        final String queueName2= getTestName() + "_2";
        _admin.createQueue(queueName1);
        _admin.createQueue(queueName2);
        _admin.afterTestMethod(null, null);
        verify(_queueAdmin).deleteQueue(_admin, queueName1);
        verify(_queueAdmin).deleteQueue(_admin, queueName2);
    }

    @Test
    public void beforeTestMethod()
    {
        _admin.beforeTestMethod(null, null);
        verifyNoInteractions(_queueAdmin);
    }

}
