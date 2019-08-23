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

package org.apache.qpid.tests.protocol.v1_0;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminException;
import org.apache.qpid.tests.utils.EmbeddedBrokerPerClassAdminImpl;

public class ExistingQueueAdminTest extends UnitTestBase
{
    private static BrokerAdmin _brokerAdmin;

    private ExistingQueueAdmin _queueAdmin;
    private String _testQueueName;

    @BeforeClass
    public static void beforeSuite()
    {
        _brokerAdmin = new EmbeddedBrokerPerClassAdminImpl();
        _brokerAdmin.beforeTestClass(ExistingQueueAdminTest.class);
    }

    @AfterClass
    public static void afterSuite()
    {
        _brokerAdmin.afterTestClass(ExistingQueueAdminTest.class);
    }

    @Before
    public void before() throws NoSuchMethodException
    {
        _brokerAdmin.beforeTestMethod(getClass(), getClass().getMethod(getTestName()));
        _brokerAdmin.createQueue(getTestName());
        _queueAdmin = new ExistingQueueAdmin();
        _testQueueName = getTestName();
    }

    @After
    public void after() throws NoSuchMethodException
    {
        _brokerAdmin.afterTestMethod(getClass(), getClass().getMethod(getTestName()));
    }


    @Test
    public void createQueue()
    {
        _queueAdmin.createQueue(_brokerAdmin, getTestName());
    }

    @Test
    public void deleteQueue() throws Exception
    {
        final String[] messages = Utils.createTestMessageContents(2, _testQueueName);
        _brokerAdmin.putMessageOnQueue(_testQueueName, messages);

        _queueAdmin.deleteQueue(_brokerAdmin, _testQueueName);

        final String controlMessage = String.format("controlMessage %s", _testQueueName);
        _brokerAdmin.putMessageOnQueue(_testQueueName, controlMessage);
        assertEquals(controlMessage, Utils.receiveMessage(_brokerAdmin, _testQueueName));
    }

    @Test
    public void deleteQueueNonExisting()
    {
        try
        {
            _queueAdmin.deleteQueue(_brokerAdmin, _testQueueName + "_NonExisting");
            fail("Exception is expected");
        }
        catch (BrokerAdminException e)
        {
            // pass
        }
    }

    @Test
    public void putMessageOnQueue() throws Exception
    {
        final String[] messages = Utils.createTestMessageContents(2, _testQueueName);
        _queueAdmin.putMessageOnQueue(_brokerAdmin, _testQueueName, messages);
        assertEquals(messages[0], Utils.receiveMessage(_brokerAdmin, _testQueueName));
        assertEquals(messages[1], Utils.receiveMessage(_brokerAdmin, _testQueueName));
    }

    @Test
    public void putMessageOnQueueNonExisting()
    {
        final String[] messages = Utils.createTestMessageContents(2, _testQueueName);
        try
        {
            _queueAdmin.putMessageOnQueue(_brokerAdmin, _testQueueName + "_NonExisting", messages);
            fail("Exception is expected"); }
        catch (BrokerAdminException e)
        {
            // pass
        }
    }

    @Test
    public void isDeleteQueueSupported()
    {
        assertFalse(_queueAdmin.isDeleteQueueSupported());
    }

    @Test
    public void isPutMessageOnQueueSupported()
    {
        assertTrue(_queueAdmin.isPutMessageOnQueueSupported());
    }
}
