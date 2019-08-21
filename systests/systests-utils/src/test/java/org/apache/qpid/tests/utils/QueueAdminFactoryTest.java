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

import static org.apache.qpid.tests.utils.QueueAdminFactory.QUEUE_ADMIN_TYPE_PROPERTY_NAME;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class QueueAdminFactoryTest extends UnitTestBase
{
    private QueueAdminFactory _factory;
    private String _preservedAdminType;

    @Before
    public void setUp()
    {
        _factory = new QueueAdminFactory();
        _preservedAdminType = System.getProperty(QUEUE_ADMIN_TYPE_PROPERTY_NAME);
    }

    @After
    public void tearDown()
    {
        if (_preservedAdminType == null)
        {
            System.clearProperty(QUEUE_ADMIN_TYPE_PROPERTY_NAME);
        }
        else
        {
            System.setProperty(QUEUE_ADMIN_TYPE_PROPERTY_NAME, _preservedAdminType);
        }
    }

    @Test
    public void testQueueAdminCreationForNonExistingType()
    {
        System.setProperty(QUEUE_ADMIN_TYPE_PROPERTY_NAME, "foo");
        try
        {
            _factory.create();
            fail("Exception is expected");
        }
        catch (BrokerAdminException e)
        {
            // pass
        }
    }

    @Test
    public void testQueueAdminCreationForExistingTypeWithPrivateConstructor()
    {
        System.setProperty(QUEUE_ADMIN_TYPE_PROPERTY_NAME, TestQueueAdmin2.class.getName());
        try
        {
            _factory.create();
            fail("Exception is expected");
        }
        catch (BrokerAdminException e)
        {
            // pass
        }
    }

    @Test
    public void testQueueAdminCreationForExistingTypeThrowingInstantiationException()
    {
        System.setProperty(QUEUE_ADMIN_TYPE_PROPERTY_NAME, TestQueueAdmin3.class.getName());
        try
        {
            _factory.create();
            fail("Exception is expected");
        }
        catch (BrokerAdminException e)
        {
            // pass
        }
    }

    @Test
    public void testQueueAdminCreationForExistingType()
    {
        System.setProperty(QUEUE_ADMIN_TYPE_PROPERTY_NAME, TestQueueAdmin.class.getName());
        final QueueAdmin admin = _factory.create();
        assertTrue(admin instanceof TestQueueAdmin);
    }

    @SuppressWarnings("WeakerAccess")
    public static class TestQueueAdmin extends NoOpQueueAdmin
    {

    }

    @SuppressWarnings("WeakerAccess")
    public static class TestQueueAdmin2 extends NoOpQueueAdmin
    {
        private TestQueueAdmin2()
        {
        }
    }

    public static class TestQueueAdmin3 extends NoOpQueueAdmin
    {
        public TestQueueAdmin3() throws InstantiationException
        {
            throw new InstantiationException("Test");
        }
    }
}
