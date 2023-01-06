/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 *
 */
package org.apache.qpid.systests.jms_1_1.connection;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;

public class ExceptionListenerTest extends JmsTestBase
{
    @Test
    public void testExceptionListenerHearsBrokerShutdown() throws Exception
    {
        assumeTrue(getBrokerAdmin().supportsRestart());

        final CountDownLatch exceptionReceivedLatch = new CountDownLatch(1);
        final AtomicReference<JMSException> exceptionHolder = new AtomicReference<>();
        Connection connection = getConnection();
        try
        {
            connection.setExceptionListener(exception -> {
                exceptionHolder.set(exception);
                exceptionReceivedLatch.countDown();
            });

            getBrokerAdmin().restart();

            assertTrue(exceptionReceivedLatch.await(getReceiveTimeout(), TimeUnit.MILLISECONDS),
                    "Exception was not propagated into exception listener in timely manner");
            assertNotNull(exceptionHolder.get(), "Unexpected exception");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testExceptionListenerClosesConnectionIsAllowed() throws  Exception
    {
        assumeTrue(getBrokerAdmin().supportsRestart());

        final Connection connection = getConnection();
        try
        {
            final CountDownLatch exceptionReceivedLatch = new CountDownLatch(1);
            final AtomicReference<JMSException> exceptionHolder = new AtomicReference<>();
            final AtomicReference<Throwable> unexpectedExceptionHolder = new AtomicReference<>();
            final ExceptionListener listener = exception -> {
                exceptionHolder.set(exception);
                try
                {
                    connection.close();
                    // PASS
                }
                catch (Throwable t)
                {
                    unexpectedExceptionHolder.set(t);
                }
                finally
                {
                    exceptionReceivedLatch.countDown();
                }
            };
            connection.setExceptionListener(listener);

            getBrokerAdmin().restart();

            assertTrue(exceptionReceivedLatch.await(getReceiveTimeout(), TimeUnit.MILLISECONDS),
                    "Exception was not propagated into exception listener in timely manner");
            assertNotNull(exceptionHolder.get(), "Unexpected exception");
            assertNull(unexpectedExceptionHolder.get(),
                    "Connection#close() should not have thrown exception");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testExceptionListenerStopsConnection_ThrowsIllegalStateException() throws  Exception
    {
        assumeTrue(getBrokerAdmin().supportsRestart());

        final Connection connection = getConnection();
        try
        {
            final CountDownLatch exceptionReceivedLatch = new CountDownLatch(1);
            final AtomicReference<JMSException> exceptionHolder = new AtomicReference<>();
            final AtomicReference<Throwable> unexpectedExceptionHolder = new AtomicReference<>();
            final ExceptionListener listener = exception -> {
                exceptionHolder.set(exception);
                try
                {
                    connection.stop();
                    fail("Exception not thrown");
                }
                catch (IllegalStateException ise)
                {
                    // PASS
                }
                catch (Throwable t)
                {
                    unexpectedExceptionHolder.set(t);
                }
                finally
                {
                    exceptionReceivedLatch.countDown();
                }
            };
            connection.setExceptionListener(listener);

            getBrokerAdmin().restart();

            assertTrue(exceptionReceivedLatch.await(getReceiveTimeout(), TimeUnit.MILLISECONDS),
                    "Exception was not propagated into exception listener in timely manner");
            assertNotNull(exceptionHolder.get(), "Unexpected exception");
            assertNull(unexpectedExceptionHolder.get(),
                    "Connection#stop() should not have thrown exception");
        }
        finally
        {
            connection.close();
        }
    }
}
