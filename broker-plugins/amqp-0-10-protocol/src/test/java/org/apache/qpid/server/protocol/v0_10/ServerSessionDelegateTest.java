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
package org.apache.qpid.server.protocol.v0_10;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.protocol.v0_10.transport.ExchangeDelete;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionErrorCode;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionException;
import org.apache.qpid.server.protocol.v0_10.transport.Option;
import org.apache.qpid.test.utils.UnitTestBase;

public class ServerSessionDelegateTest extends UnitTestBase
{
    private VirtualHost<?> _host;
    private ServerSession _session;
    private ServerSessionDelegate _delegate;

    @Before
    public void setUp() throws Exception
    {
        _host = mock(VirtualHost.class);

        ServerConnection serverConnection = mock(ServerConnection.class);
        doReturn(_host).when(serverConnection).getAddressSpace();

        _session = mock(ServerSession.class);
        when(_session.getConnection()).thenReturn(serverConnection);

        _delegate = new ServerSessionDelegate();
    }

    @Test
    public void testExchangeDeleteWhenIfUsedIsSetAndExchangeHasBindings() throws Exception
    {
        Exchange<?> exchange = mock(Exchange.class);
        when(exchange.hasBindings()).thenReturn(true);

        doReturn(exchange).when(_host).getAttainedMessageDestination(eq(getTestName()), anyBoolean());

        final ExchangeDelete method = new ExchangeDelete(getTestName(), Option.IF_UNUSED);
        _delegate.exchangeDelete(_session, method);

        verify(_session).invoke(argThat((ArgumentMatcher<ExecutionException>) exception -> exception.getErrorCode()
                                                                                           == ExecutionErrorCode.PRECONDITION_FAILED
                                                                                           && "Exchange has bindings".equals(
                exception.getDescription())));
    }

    @Test
    public void testExchangeDeleteWhenIfUsedIsSetAndExchangeHasNoBinding() throws Exception
    {
        Exchange<?> exchange = mock(Exchange.class);
        when(exchange.hasBindings()).thenReturn(false);

        doReturn(exchange).when(_host).getAttainedMessageDestination(eq(getTestName()), anyBoolean());

        final ExchangeDelete method = new ExchangeDelete(getTestName(), Option.IF_UNUSED);
        _delegate.exchangeDelete(_session, method);

        verify(exchange).delete();
    }

}
