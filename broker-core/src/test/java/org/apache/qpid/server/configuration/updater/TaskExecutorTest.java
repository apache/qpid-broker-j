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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.test.utils.UnitTestBase;

public class TaskExecutorTest extends UnitTestBase
{
    private TaskExecutorImpl _executor;

    @BeforeEach
    public void setUp() throws Exception
    {
        _executor = new TaskExecutorImpl();
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        _executor.stopImmediately();
    }

    @Test
    public void testGetState()
    {
        assertFalse(_executor.isRunning(), "Unexpected initial state");
    }

    @Test
    public void testStart()
    {
        _executor.start();
        assertTrue(_executor.isRunning(), "Unexpected started state");
    }

    @Test
    public void testStopImmediately() throws Exception
    {
        _executor.start();
        final CountDownLatch submitLatch = new CountDownLatch(2);
        final CountDownLatch waitForCallLatch = new CountDownLatch(1);
        final BlockingQueue<Exception> submitExceptions = new LinkedBlockingQueue<>();

        final Runnable runnable = () ->
        {
            try
            {
                final Future<Void> f = _executor.submit(new NeverEndingCallable(waitForCallLatch));
                submitLatch.countDown();
                f.get();
            }
            catch (Exception e)
            {
                Exception exception = e;
                if (exception instanceof ExecutionException)
                {
                    exception = (Exception) exception.getCause();
                }
                if (exception instanceof RuntimeException && exception.getCause() instanceof Exception)
                {
                    submitExceptions.add((Exception) exception.getCause());
                }
                else
                {
                    submitExceptions.add(exception);
                }
            }
        };
        final Thread t1 = new Thread(runnable);
        t1.start();
        final Thread t2 = new Thread(runnable);
        t2.start();

        final long timeout = 2000L;
        final boolean awaitSubmissions = submitLatch.await(timeout, TimeUnit.MILLISECONDS);
        assertTrue(awaitSubmissions, submitLatch.getCount() +
                " task(s) have not been submitted within expected time");

        assertTrue(waitForCallLatch.await(timeout, TimeUnit.MILLISECONDS), "The first task has not been triggered");

        _executor.stopImmediately();
        assertFalse(_executor.isRunning(), "Unexpected stopped state");

        final Exception e = submitExceptions.poll(timeout, TimeUnit.MILLISECONDS);
        assertNotNull(e, "The task execution was not interrupted or cancelled");
        final Exception e2 = submitExceptions.poll(timeout, TimeUnit.MILLISECONDS);
        assertNotNull(e2, "The task execution was not interrupted or cancelled");

        final boolean condition1 = e2 instanceof CancellationException || e instanceof CancellationException;
        assertTrue(condition1, "One of the exceptions should be CancellationException:");
        final boolean condition = e2 instanceof InterruptedException || e instanceof InterruptedException;
        assertTrue(condition, "One of the exceptions should be InterruptedException:");

        t1.join(timeout);
        t2.join(timeout);
    }

    @Test
    public void testStop()
    {
        _executor.start();
        _executor.stop();
        assertFalse(_executor.isRunning(), "Unexpected stopped state");
    }

    @Test
    public void testSubmitAndWait()
    {
        _executor.start();
        final Object result = _executor.run(new Task<String, RuntimeException>()
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
        assertEquals("DONE", result, "Unexpected task execution result");
    }

    @Test
    public void testSubmitAndWaitInNotAuthorizedContext()
    {
        _executor.start();
        final Object subject = _executor.run(new SubjectRetriever());
        assertNull(subject, "Subject must be null");
    }

    @Test
    public void testSubmitAndWaitInAuthorizedContext()
    {
        _executor.start();
        final Subject subject = new Subject();
        final Object result = Subject.doAs(subject, (PrivilegedAction<Object>) () ->
                _executor.run(new SubjectRetriever()));
        assertEquals(subject, result, "Unexpected subject");
    }

    @Test
    public void testSubmitAndWaitInAuthorizedContextWithNullSubject()
    {
        _executor.start();
        final Object result = Subject.doAs(null, (PrivilegedAction<Object>) () ->
                _executor.run(new SubjectRetriever()));
        assertNull(result, "Unexpected subject");
    }

    @Test
    public void testSubmitAndWaitReThrowsOriginalRuntimeException()
    {
        final RuntimeException exception = new RuntimeException();
        _executor.start();
        final Exception thrown = assertThrows(Exception.class,
                () -> _executor.run(new Task<Void, RuntimeException>()
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
                }),
                "Exception is expected");
        assertEquals(exception, thrown, "Unexpected exception");
    }

    @Test
    public void testSubmitAndWaitCurrentActorAndSecurityManagerSubjectAreRespected()
    {
        _executor.start();
        final Subject subject = new Subject();
        final AtomicReference<Subject> taskSubject = new AtomicReference<>();
        Subject.doAs(subject, (PrivilegedAction<Object>) () ->
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
        });

        assertEquals(subject, taskSubject.get(), "Unexpected security manager subject");
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
        private final CountDownLatch _waitLatch;

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
