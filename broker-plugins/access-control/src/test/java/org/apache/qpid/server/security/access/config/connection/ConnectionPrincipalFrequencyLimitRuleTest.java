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
 */

package org.apache.qpid.server.security.access.config.connection;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import javax.security.auth.Subject;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.test.utils.UnitTestBase;

public class ConnectionPrincipalFrequencyLimitRuleTest extends UnitTestBase
{

    private AMQPConnection _connection;
    private Subject _subject;

    @Before
    public void setUp()
    {
        _connection = mock(AMQPConnection.class);

        final ConnectionPrincipal connectionPrincipal = mock(ConnectionPrincipal.class);
        when(connectionPrincipal.getConnection()).thenReturn(_connection);
        when(_connection.getAuthorizedPrincipal()).thenReturn(connectionPrincipal);
        _subject = new Subject(false,
                               Collections.singleton(connectionPrincipal),
                               Collections.emptySet(),
                               Collections.emptySet());
    }

    @Test
    public void limitBreached()
    {
        when(_connection.getAuthenticatedPrincipalConnectionFrequency()).thenReturn(2);
        final ConnectionPrincipalFrequencyLimitRule rule = new ConnectionPrincipalFrequencyLimitRule(1);

        assertThat(rule.matches(_subject), is(false));
    }

    @Test
    public void frequencyEqualsLimit()
    {
        when(_connection.getAuthenticatedPrincipalConnectionFrequency()).thenReturn(2);
        final ConnectionPrincipalFrequencyLimitRule rule = new ConnectionPrincipalFrequencyLimitRule(2);

        assertThat(rule.matches(_subject), is(true));
    }

    @Test
    public void frequencyBelowLimit()
    {
        when(_connection.getAuthenticatedPrincipalConnectionFrequency()).thenReturn(1);
        final ConnectionPrincipalFrequencyLimitRule rule = new ConnectionPrincipalFrequencyLimitRule(2);

        assertThat(rule.matches(_subject), is(true));
    }
}
