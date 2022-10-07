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

import java.security.Principal;

import org.junit.Before;

import org.apache.qpid.server.transport.AMQPConnection;

/**
 * Validate ConnectionLogSubjects are logged as expected
 */
public class ConnectionLogSubjectTest extends AbstractTestLogSubject
{

    private static final long CONNECTION_ID = 456L;
    private static final String USER = "InternalTestProtocolSession";
    private static final String IP_STRING = "127.0.0.1:1";
    private static final String VHOST = "test";

    private AMQPConnection _connection;

    @Before
    public void setUp() throws Exception
    {
        super.setUp();

        final Principal principal = mock(Principal.class);
        when(principal.getName()).thenReturn(USER);

        _connection = mock(AMQPConnection.class);
        when(_connection.getConnectionId()).thenReturn(CONNECTION_ID);
        when(_connection.getAuthorizedPrincipal()).thenReturn(principal);
        when(_connection.getRemoteAddressString()).thenReturn("/"+IP_STRING);
        when(_connection.getAddressSpaceName()).thenReturn(VHOST);
        _subject = new ConnectionLogSubject(_connection);
    }

    /**
     * MESSAGE [Blank][con:0(MockProtocolSessionUser@null/test)] <Log Message>
     *
     * @param message the message whose format needs validation
     */
    @Override
    protected void validateLogStatement(String message)
    {
        verifyConnection(CONNECTION_ID, USER, IP_STRING, VHOST, message);
    }

    public AMQPConnection getConnection()
    {
        return _connection;
    }

}
