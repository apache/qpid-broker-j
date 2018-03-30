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
package org.apache.qpid.server.logging.subjects;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;

import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

/**
 * Validate BindingLogSubjects are logged as expected
 */
public class BindingLogSubjectTest extends AbstractTestLogSubject
{

    private Queue<?> _queue;
    private String _routingKey;
    private Exchange<?> _exchange;
    private QueueManagingVirtualHost _testVhost;

    @Before
    public void setUp() throws Exception
    {
        super.setUp();

        _testVhost = BrokerTestHelper.createVirtualHost("test", this);
        _routingKey = "RoutingKey";
        _exchange = (Exchange<?>) _testVhost.getChildByName(Exchange.class, "amq.direct");
        _queue = mock(Queue.class);
        when(_queue.getName()).thenReturn("BindingLogSubjectTest");
        when(_queue.getVirtualHost()).thenReturn(_testVhost);

        _subject = new BindingLogSubject(_routingKey, _exchange, _queue);
    }

    @After
    public void tearDown() throws Exception
    {
        if (_testVhost != null)
        {
            _testVhost.close();
        }
        super.tearDown();
    }

    /**
     * Validate that the logged Subject  message is as expected:
     * MESSAGE [Blank][vh(/test)/ex(direct/<<default>>)/qu(BindingLogSubjectTest)/rk(RoutingKey)] <Log Message>
     * @param message the message whose format needs validation
     */
    @Override
    protected void validateLogStatement(String message)
    {
        verifyVirtualHost(message, _testVhost);
        verifyExchange(message, _exchange);
        verifyQueue(message, _queue);
        verifyRoutingKey(message, _routingKey);
    }


}
