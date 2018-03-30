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
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

/**
 * Validate QueueLogSubjects are logged as expected
 */
public class QueueLogSubjectTest extends AbstractTestLogSubject
{

    private Queue<?> _queue;
    private QueueManagingVirtualHost _testVhost;

    @Before
    public void setUp() throws Exception
    {
        super.setUp();

        _testVhost = BrokerTestHelper.createVirtualHost("test", this);

        _queue = mock(Queue.class);
        when(_queue.getName()).thenReturn("QueueLogSubjectTest");
        when(_queue.getVirtualHost()).thenReturn(_testVhost);

        _subject = new QueueLogSubject(_queue);
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
     * MESSAGE [Blank][vh(/test)/qu(QueueLogSubjectTest)] <Log Message>
     *
     * @param message the message whose format needs validation
     */
    @Override
    protected void validateLogStatement(String message)
    {
        verifyVirtualHost(message, _testVhost);
        verifyQueue(message, _queue);
    }
}
