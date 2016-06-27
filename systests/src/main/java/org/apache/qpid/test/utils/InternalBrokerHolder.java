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
package org.apache.qpid.test.utils;

import java.io.File;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.Broker;
import org.apache.qpid.server.BrokerOptions;
import org.apache.qpid.server.security.SecurityManager;
import org.apache.qpid.server.util.Action;

public class InternalBrokerHolder extends AbstractBrokerHolder
{
    private final static Logger LOGGER = LoggerFactory.getLogger(InternalBrokerHolder.class);
    private final static UncaughtExceptionHandler UNCAUGHT_EXCEPTION_HANDLER = new UncaughtExceptionHandler();

    private volatile Broker _broker;

    public InternalBrokerHolder(int port, final String classQualifiedTestName, final File logFile)
    {
        super(port, classQualifiedTestName, logFile);
    }

    @Override
    public void start(final BrokerOptions options) throws Exception
    {
        if (Thread.getDefaultUncaughtExceptionHandler() == null)
        {
            Thread.setDefaultUncaughtExceptionHandler(UNCAUGHT_EXCEPTION_HANDLER);
        }

        LOGGER.info("Starting internal broker (same JVM)");

        _broker = new Broker(new Action<Integer>()
        {
            @Override
            public void performAction(final Integer object)
            {
                _broker = null;
            }
        })
        {
            @Override
            public void shutdown()
            {
                try
                {
                    super.shutdown();
                }
                catch (IllegalStateException e)
                {
                    System.out.println("IllegalStateException occurred on broker shutdown in test "
                                       + getClassQualifiedTestName());
                    throw e;
                }
            }
        };

        _broker.startup(options);
    }

    @Override
    public void shutdown()
    {
        if (_broker != null)
        {
            LOGGER.info("Shutting down Broker instance");

            Subject.doAs(SecurityManager.getSystemTaskSubject("Shutdown"), new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    if (_broker != null)
                    {
                        _broker.shutdown();
                    }
                    return null;
                }
            });
            waitUntilPortsAreFree();
            LOGGER.info("Broker instance shutdown");
        }
        else
        {
            LOGGER.info("Nothing to shutdown. Broker instance either was already shut down or not started at all.");
        }

        if (UNCAUGHT_EXCEPTION_HANDLER.getAndResetCount() > 0)
        {
            throw new RuntimeException(
                    "One or more uncaught exceptions occurred prior to end of this test. Check test logs.");
        }
    }

    @Override
    public void kill()
    {
        // Can't kill a internal broker as we would also kill ourselves as we share the same JVM.
        shutdown();
    }

    @Override
    public String dumpThreads()
    {
        return TestUtils.dumpThreads();
    }

    @Override
    public String toString()
    {
        return "InternalBrokerHolder [amqpPort=" + getAmqpPort() + "]";
    }

    @Override
    protected String getLogPrefix()
    {
        return null;
    }

    private static class UncaughtExceptionHandler implements Thread.UncaughtExceptionHandler
    {
        private final AtomicInteger _count = new AtomicInteger(0);

        @Override
        public void uncaughtException(final Thread t, final Throwable e)
        {
            System.err.print("Thread terminated due to uncaught exception");
            e.printStackTrace();

            LOGGER.error("Uncaught exception from thread {}", t.getName(), e);
            _count.getAndIncrement();
        }

        public int getAndResetCount()
        {
            int count;
            do
            {
                count = _count.get();
            }
            while (!_count.compareAndSet(count, 0));
            return count;
        }
    }
}
