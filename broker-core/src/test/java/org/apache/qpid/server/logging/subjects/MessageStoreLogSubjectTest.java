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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.VirtualHost;

/**
 * Validate MessageStoreLogSubjects are logged as expected
 */
public class MessageStoreLogSubjectTest extends AbstractTestLogSubject
{
    private VirtualHost<?> _testVhost;

    @BeforeAll
    public void beforeAll() throws Exception
    {
        _testVhost = BrokerTestHelper.createVirtualHost("test", this);
    }

    @BeforeEach
    public void setUp() throws Exception
    {
        _subject = new MessageStoreLogSubject(_testVhost.getName(), _testVhost.getMessageStore().getClass().getSimpleName());
    }

    /**
     * Validate that the logged Subject  message is as expected:
     * MESSAGE [Blank][vh(/test)/ms(MemoryMessageStore)] <Log Message>
     * @param message the message who's format needs validation
     */
    @Override
    protected void validateLogStatement(final String message)
    {
        verifyVirtualHost(message, _testVhost);

        final String msSlice = getSlice("ms", message);

        assertNotNull(msSlice, "MessageStore not found:" + message);

        assertEquals(_testVhost.getMessageStore().getClass().getSimpleName(), msSlice, "MessageStore not correct");
    }
}
