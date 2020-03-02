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

package org.apache.qpid.server.virtualhost.connection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;

import javax.security.auth.Subject;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.ConnectionStatisticsRegistrySettings;
import org.apache.qpid.test.utils.UnitTestBase;

public class ConnectionPrincipalStatisticsRegistryImplTest extends UnitTestBase
{
    private static final Duration CONNECTION_FREQUENCY_PERIOD = Duration.ofMillis(5000);
    private ConnectionPrincipalStatisticsRegistryImpl _statisticsRegistry;
    private AuthenticatedPrincipal _authorizedPrincipal;
    private ConnectionStatisticsRegistrySettings _settings;

    @Before
    public void setUp()
    {
        _settings = mock(ConnectionStatisticsRegistrySettings.class);
        when(_settings.getConnectionFrequencyPeriod()).thenReturn(CONNECTION_FREQUENCY_PERIOD);
        _statisticsRegistry = new ConnectionPrincipalStatisticsRegistryImpl(_settings);
        _authorizedPrincipal = new AuthenticatedPrincipal(mock(Principal.class));
    }

    @Test
    public void onConnectionOpen()
    {
        final AMQPConnection connection = mockConnection();

        _statisticsRegistry.connectionOpened(connection);

        assertThat(_statisticsRegistry.getConnectionCount(_authorizedPrincipal), is(equalTo(1)));
        assertThat(_statisticsRegistry.getConnectionCount(_authorizedPrincipal), is(equalTo(1)));
    }

    @Test
    public void onConnectionClose()
    {
        final AMQPConnection connection1 = mockConnection();

        _statisticsRegistry.connectionOpened(connection1);
        _statisticsRegistry.connectionClosed(connection1);

        final AMQPConnection connection2 = mockConnection();
        _statisticsRegistry.connectionOpened(connection2);

        assertThat(_statisticsRegistry.getConnectionCount(_authorizedPrincipal), is(equalTo(1)));
        assertThat(_statisticsRegistry.getConnectionFrequency(_authorizedPrincipal), is(equalTo(2)));
    }

    @Test
    public void reevaluateConnectionPrincipalStatistics() throws InterruptedException
    {
        final AMQPConnection connection1 = mockConnection();

        _statisticsRegistry.connectionOpened(connection1);
        assertThat(_statisticsRegistry.getConnectionFrequency(_authorizedPrincipal), is(equalTo(1)));

        _statisticsRegistry.reevaluateConnectionStatistics();
        assertThat(_statisticsRegistry.getConnectionFrequency(_authorizedPrincipal), is(equalTo(1)));

        when(_settings.getConnectionFrequencyPeriod()).thenReturn(Duration.ofMillis(1));
        Thread.sleep(_settings.getConnectionFrequencyPeriod().toMillis() + 1);

        _statisticsRegistry.reevaluateConnectionStatistics();
        assertThat(_statisticsRegistry.getConnectionCount(_authorizedPrincipal), is(equalTo(1)));
        assertThat(_statisticsRegistry.getConnectionFrequency(_authorizedPrincipal), is(equalTo(0)));
    }

    @Test
    public void getConnectionFrequencyAfterExpirationOfFrequencyPeriod() throws InterruptedException
    {
        final AMQPConnection connection1 = mockConnection();
        _statisticsRegistry.connectionOpened(connection1);

        assertThat(_statisticsRegistry.getConnectionFrequency(_authorizedPrincipal), is(equalTo(1)));
        assertThat(_statisticsRegistry.getConnectionCount(_authorizedPrincipal), is(equalTo(1)));

        when(_settings.getConnectionFrequencyPeriod()).thenReturn(Duration.ofMillis(1));
        Thread.sleep(_settings.getConnectionFrequencyPeriod().toMillis() + 1);

        final AMQPConnection connection2 = mockConnection();
        _statisticsRegistry.connectionOpened(connection2);

        assertThat(_statisticsRegistry.getConnectionCount(_authorizedPrincipal), is(equalTo(2)));
        assertThat(_statisticsRegistry.getConnectionFrequency(_authorizedPrincipal), is(equalTo(1)));
    }

    private AMQPConnection mockConnection()
    {
        final Subject subject = new Subject(true,
                                            Collections.singleton(_authorizedPrincipal),
                                            Collections.emptySet(),
                                            Collections.emptySet());

        final AMQPConnection connection = mock(AMQPConnection.class);
        when(connection.getAuthorizedPrincipal()).thenReturn(_authorizedPrincipal);
        when(connection.getSubject()).thenReturn(subject);
        when(connection.getCreatedTime()).thenReturn(new Date());
        return connection;
    }
}
