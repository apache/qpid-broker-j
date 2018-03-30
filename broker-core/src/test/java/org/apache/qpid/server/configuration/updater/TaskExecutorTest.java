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
package org.apache.qpid.server.configuration.updater;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.test.utils.UnitTestBase;

public class TaskExecutorTest extends UnitTestBase
{
    private TaskExecutorImpl _executor;

    @Before
    public void setUp() throws Exception
    {
        _executor = new TaskExecutorImpl();
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            _executor.stopImmediately();
        }
        finally
        {
        }
    }

    @Test
    public void testGetState()
    {
        assertFalse("Unexpected initial state", _executor.isRunning());
    }

    @Test
    public void testStart()
    {
        _executor.start();
        assertTrue("Unexpected started state", _executor.isRunning());
    }

    @Test
    public void testStopImmediately() throws Exception
    {
        _executor.start();
        final CountDownLatch submitLatch = new CountDownLatch(2);
        final CountDownLatch waitForCallLatch = new CountDownLatch(1);
        final BlockingQueue<Exception> submitExceptions = new LinkedBlockingQueue<Exception>();

        Runnable runnable = new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    Future<Void> f = _executor.submit(new NeverEndingCallable(waitForCallLatch));
                    submitLatch.countDown();
                    f.get();
                }
                catch (Exception e)
                {
                    if (e instanceof ExecutionException)
                    {
                        e = (Exception) e.getCause();
                    }
                    if(e instanceof RuntimeException && e.getCause() instanceof Exception)
                    {
                        submitExceptions.add((Exception)e.getCause());
                    }
                    else
                    {
                        submitExceptions.add(e);
                    }
                }
            }
        };
        Thread t1 = new Thread(runnable);
        t1.start();
        Thread t2 = new Thread(runnable);
        t2.start();

        final long timeout = 2000l;
        boolean awaitSubmissions = submitLatch.await(timeout, TimeUnit.MILLISECONDS);
        assertTrue(submitLatch.getCount() + " task(s) have not been submitted within expected time",
                          awaitSubmissions);

        assertTrue("The first task has not been triggered",
                          waitForCallLatch.await(timeout, TimeUnit.MILLISECONDS));

        _executor.stopImmediately();
        assertFalse("Unexpected stopped state", _executor.isRunning());

        Exception e = submitExceptions.poll(timeout, TimeUnit.MILLISECONDS);
        assertNotNull("The task execution was not interrupted or cancelled", e);
        Exception e2 = submitExceptions.poll(timeout, TimeUnit.MILLISECONDS);
        assertNotNull("The task execution was not interrupted or cancelled", e2);

        final boolean condition1 = e2 instanceof CancellationException
                || e instanceof CancellationException;
        assertTrue("One of the exceptions should be CancellationException:", condition1);
        final boolean condition = e2 instanceof InterruptedException
                || e instanceof InterruptedException;
        assertTrue("One of the exceptions should be InterruptedException:", condition);

        t1.join(timeout);
        t2.join(timeout);
    }

    @Test
    public void testStop()
    {
        _executor.start();
        _executor.stop();
        assertFalse("Unexpected stopped state", _executor.isRunning());
    }

    @Test
    public void testSubmitAndWait() throws Exception
    {
        _executor.start();
        Object result = _executor.run(new Task<String, RuntimeException>()
        {
            @Override
            public String execute()
            {
                return "DONE";
            }

            @Override
            public String getObject()
            {
                return getTestName();
            }

            @Override
            public String getAction()
            {
                return "test";
            }

            @Override
            public String getArguments()
            {
                return null;
            }
        });
        assertEquals("Unexpected task execution result", "DONE", result);
    }

    @Test
    public void testSubmitAndWaitInNotAuthorizedContext()
    {
        _executor.start();
        Object subject = _executor.run(new SubjectRetriever());
        assertNull("Subject must be null", subject);
    }

    @Test
    public void testSubmitAndWaitInAuthorizedContext()
    {
        _executor.start();
        Subject subject = new Subject();
        Object result = Subject.doAs(subject, new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                return _executor.run(new SubjectRetriever());
            }
        });
        assertEquals("Unexpected subject", subject, result);
    }

    @Test
    public void testSubmitAndWaitInAuthorizedContextWithNullSubject()
    {
        _executor.start();
        Object result = Subject.doAs(null, new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                return _executor.run(new SubjectRetriever());
            }
        });
        assertEquals("Unexpected subject", null, result);
    }

    @Test
    public void testSubmitAndWaitReThrowsOriginalRuntimeException()
    {
        final RuntimeException exception = new RuntimeException();
        _executor.start();
        try
        {
            _executor.run(new Task<Void, RuntimeException>()
            {

                @Override
                public Void execute()
                {
                    throw exception;
                }

                @Override
                public String getObject()
                {
                    return getTestName();
                }

                @Override
                public String getAction()
                {
                    return "test";
                }

                @Override
                public String getArguments()
                {
                    return null;
                }
            });
            fail("Exception is expected");
        }
        catch (Exception e)
        {
            assertEquals("Unexpected exception", exception, e);
        }
    }

    @Test
    public void testSubmitAndWaitCurrentActorAndSecurityManagerSubjectAreRespected() throws Exception
    {
        _executor.start();
        Subject subject = new Subject();
        final AtomicReference<Subject> taskSubject = new AtomicReference<Subject>();
        Subject.doAs(subject, new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                _executor.run(new Task<Void, RuntimeException>()
                {
                    @Override
                    public Void execute()
                    {
                        taskSubject.set(Subject.getSubject(AccessController.getContext()));
                        return null;
                    }

                    @Override
                    public String getObject()
                    {
                        return getTestName();
                    }

                    @Override
                    public String getAction()
                    {
                        return "test";
                    }

                    @Override
                    public String getArguments()
                    {
                        return null;
                    }
                });
                return null;
            }
        });

        assertEquals("Unexpected security manager subject", subject, taskSubject.get());
    }

    private class SubjectRetriever implements Task<Subject, RuntimeException>
    {
        @Override
        public Subject execute()
        {
            return Subject.getSubject(AccessController.getContext());
        }

        @Override
        public String getObject()
        {
            return getTestName();
        }

        @Override
        public String getAction()
        {
            return "test";
        }

        @Override
        public String getArguments()
        {
            return null;
        }
    }

    private class NeverEndingCallable implements Task<Void, RuntimeException>
    {
        private CountDownLatch _waitLatch;

        public NeverEndingCallable(CountDownLatch waitLatch)
        {
            _waitLatch = waitLatch;
        }

        @Override
        public Void execute()
        {
            if (_waitLatch != null)
            {
                _waitLatch.countDown();
            }

            // wait forever
            synchronized (this)
            {
                try
                {
                    this.wait();
                }
                catch (InterruptedException e)
                {
                    throw new ServerScopedRuntimeException(e);
                }
            }
            return null;
        }

        @Override
        public String getObject()
        {
            return getTestName();
        }

        @Override
        public String getAction()
        {
            return "test";
        }

        @Override
        public String getArguments()
        {
            return null;
        }
    }
}
