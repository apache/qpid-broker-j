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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.security.Principal;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Set;

import javax.security.auth.Subject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.security.SubjectExecutionContext;
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
    public void testSubmitAndWaitInAuthorizedContext() throws Exception
    {
        _executor.start();
        final Subject subject = new Subject();
        final Object result = SubjectExecutionContext.withSubject(subject,
                () -> _executor.run(new SubjectRetriever()));
        assertEquals(subject, result, "Unexpected subject");
    }

    @Test
    public void testSubmitAndWaitInAuthorizedContextWithNullSubject() throws Exception
    {
        _executor.start();
        final Object result = SubjectExecutionContext.withSubject(null,
                () -> _executor.run(new SubjectRetriever()));
        assertNull(result, "Unexpected subject");
    }

    @Test
    public void testSubmitAndWaitReThrowsOriginalRuntimeException()
    {
        final RuntimeException exception = new RuntimeException();
        _executor.start();
        final Exception thrown = assertThrows(Exception.class,
                () -> _executor.run(new TestTask(exception)),
                "Exception is expected");
        assertEquals(exception, thrown, "Unexpected exception");
    }

    @Test
    public void testSubmitAndWaitCurrentSubjectIsRespected()
    {
        _executor.start();
        final Subject subject = new Subject();
        final AtomicReference<Subject> taskSubject = new AtomicReference<>();
        SubjectExecutionContext.withSubject(subject, () ->
        {
            _executor.run(new TestTask(taskSubject));
        });

        assertEquals(subject, taskSubject.get(), "Unexpected subject");
    }

    @Test
    public void testPrincipalAccessorAddsMissingPrincipal() throws Exception
    {
        final Principal userPrincipal = () -> getTestName() + "-user";
        final Principal accessorPrincipal = () -> getTestName() + "-accessor";
        final Subject subject = new Subject(true, Set.of(userPrincipal), Set.of(), Set.of());

        final TaskExecutorImpl executorWithAccessor = new TaskExecutorImpl(getTestName(), () -> accessorPrincipal);
        executorWithAccessor.start();
        try
        {
            final AtomicReference<Subject> taskSubject = new AtomicReference<>();
            SubjectExecutionContext.withSubject(subject, () ->
                    executorWithAccessor.run(new TestTask(taskSubject)));

            final Subject captured = taskSubject.get();
            assertNotNull(captured, "Subject was not captured");
            assertTrue(captured.getPrincipals().contains(userPrincipal), "Original principal missing");
            assertTrue(captured.getPrincipals().contains(accessorPrincipal), "Accessor principal missing");
            assertTrue(captured.isReadOnly(), "Subject should be read-only");
            assertFalse(subject.getPrincipals().contains(accessorPrincipal), "Original subject was mutated");
        }
        finally
        {
            executorWithAccessor.stopImmediately();
        }
    }

    @Test
    public void testPrincipalAccessorDoesNotReplaceWhenPresent() throws Exception
    {
        final Principal principal = () -> getTestName() + "-user";
        final Subject subject = new Subject(false, Set.of(principal), Set.of(), Set.of());

        final TaskExecutorImpl executorWithAccessor = new TaskExecutorImpl(getTestName(), () -> principal);
        executorWithAccessor.start();
        try
        {
            final AtomicReference<Subject> taskSubject = new AtomicReference<>();
            SubjectExecutionContext.withSubject(subject, () ->
                    executorWithAccessor.run(new Task<Void, RuntimeException>()
                    {
                        @Override
                        public Void execute()
                        {
                            taskSubject.set(SubjectExecutionContext.currentSubject());
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
                    }));

            assertSame(subject, taskSubject.get(), "Unexpected subject instance");
        }
        finally
        {
            executorWithAccessor.stopImmediately();
        }
    }

    @Test
    public void testTaskSubjectCapturedAtSubmission() throws Exception
    {
        _executor.start();

        final Subject initialSubject = _executor.run(new SubjectRetriever());
        assertNull(initialSubject, "Subject must be null before test");

        final CountDownLatch taskStarted = new CountDownLatch(1);
        final CountDownLatch taskRelease = new CountDownLatch(1);
        final CountDownLatch subjectCaptured = new CountDownLatch(1);
        final AtomicReference<Subject> capturedSubject = new AtomicReference<>();

        final Future<Void> blockingFuture = _executor.submit(new Task<Void, RuntimeException>()
        {
            @Override
            public Void execute()
            {
                taskStarted.countDown();
                try
                {
                    taskRelease.await(3, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
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
                return "block";
            }

            @Override
            public String getArguments()
            {
                return null;
            }
        });

        assertTrue(taskStarted.await(3, TimeUnit.SECONDS), "Blocking task did not start");

        final Subject subject = new Subject();
        final AtomicReference<Future<Void>> captureFuture = new AtomicReference<>();

        SubjectExecutionContext.withSubject(subject, () ->
                captureFuture.set(_executor.submit(new Task<Void, RuntimeException>()
                {
                    @Override
                    public Void execute()
                    {
                        capturedSubject.set(SubjectExecutionContext.currentSubject());
                        subjectCaptured.countDown();
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
                        return "capture";
                    }

                    @Override
                    public String getArguments()
                    {
                        return null;
                    }
                })));

        taskRelease.countDown();

        assertTrue(subjectCaptured.await(3, TimeUnit.SECONDS), "Subject was not captured");
        assertEquals(subject, capturedSubject.get(), "Unexpected subject");

        final Future<Void> submittedCapture = captureFuture.get();
        assertNotNull(submittedCapture, "Capture future was not set");
        submittedCapture.get(3, TimeUnit.SECONDS);
        blockingFuture.get(3, TimeUnit.SECONDS);
    }

    @Test
    public void testSubjectNotLeakedBetweenTasksOnSameWorkerThread() throws Exception
    {
        _executor.start();

        // 1) warmup: create a worker-thread with currentSubject = null
        assertNull(_executor.run(new SubjectRetriever()), "Warm-up must see null subject");

        // 2) execute task with admin subject
        final Subject admin = new Subject();
        final Subject seenInsideTask = SubjectExecutionContext.withSubject(admin, () -> _executor.run(new SubjectRetriever()));
        assertSame(admin, seenInsideTask, "Task must see the subject that was set at submission time");

        // 3) after task is done subject in worker-thread must be removed
        final ExecutorService raw = getUnderlyingExecutor(_executor);
        final Subject subjectAfterTask =
                raw.submit(() -> SubjectExecutionContext.currentSubject()).get(3, TimeUnit.SECONDS);

        assertNull(subjectAfterTask, "Subject leaked on worker-thread between tasks");
    }

    private static ExecutorService getUnderlyingExecutor(final TaskExecutorImpl executor) throws Exception
    {
        final Field field = TaskExecutorImpl.class.getDeclaredField("_executor");
        field.setAccessible(true);
        return (ExecutorService) field.get(executor);
    }

    @Test
    public void testSubjectNotLeakedWhenTaskThrows() throws Exception
    {
        _executor.start();

        // warmup
        assertNull(_executor.run(new SubjectRetriever()), "Warm-up must see null subject");

        final Subject admin = new Subject();

        try
        {
            SubjectExecutionContext.withSubject(admin, () ->
            {
                _executor.run(new Task<Void, RuntimeException>()
                {
                    @Override public Void execute() { throw new RuntimeException("boom"); }
                    @Override public String getObject() { return getTestName(); }
                    @Override public String getAction() { return "boom"; }
                    @Override public String getArguments() { return null; }
                });
                return null;
            });
        }
        catch (RuntimeException expected)
        {
            // ok
        }

        final ExecutorService raw = getUnderlyingExecutor(_executor);
        final Subject subjectAfterFailure = raw.submit(() -> SubjectExecutionContext.currentSubject()).get(3, TimeUnit.SECONDS);

        assertNull(subjectAfterFailure, "Subject leaked on worker-thread after exception");
    }

    @Test
    public void testWorkerThreadHasNoSubjectAfterFirstSubmission() throws Exception
    {
        _executor.start();

        final Subject admin = new Subject();

        // first submit with subject=admin
        final Subject seenInside = SubjectExecutionContext.withSubject(admin, () -> _executor.run(new SubjectRetriever()));
        assertSame(admin, seenInside);

        final ExecutorService raw = getUnderlyingExecutor(_executor);
        final Subject subjectAfter = raw.submit(() -> SubjectExecutionContext.currentSubject()).get(3, TimeUnit.SECONDS);

        assertNull(subjectAfter, "Subject leaked into worker-thread after task execution");
    }

    @Test
    public void testExecuteRunnableSubjectCapturedAtSubmissionAndNotLeaked() throws Exception
    {
        _executor.start();
        assertNull(_executor.run(new SubjectRetriever()), "Warm-up must see null subject");

        final Subject admin = new Subject();
        final CountDownLatch executed = new CountDownLatch(1);
        final AtomicReference<Subject> seenInsideRunnable = new AtomicReference<>();

        SubjectExecutionContext.withSubject(admin, () -> _executor.execute(() ->
        {
            seenInsideRunnable.set(SubjectExecutionContext.currentSubject());
            executed.countDown();
        }));

        assertTrue(executed.await(3, TimeUnit.SECONDS), "Runnable was not executed");
        assertSame(admin, seenInsideRunnable.get(), "Runnable must see subject captured at execute() call");

        final ExecutorService raw = getUnderlyingExecutor(_executor);
        final Subject subjectAfter = raw.submit(() -> SubjectExecutionContext.currentSubject()).get(3, TimeUnit.SECONDS);

        assertNull(subjectAfter, "Subject leaked on worker-thread after execute(Runnable)");
    }

    private class TestTask implements Task<Void, RuntimeException>
    {
        private final AtomicReference<Subject> _taskSubject;
        private final RuntimeException _exception;

        TestTask(final AtomicReference<Subject> taskSubject)
        {
            _taskSubject = taskSubject;
            _exception = null;
        }

        TestTask(final RuntimeException exception)
        {
            _taskSubject = null;
            _exception = exception;
        }

        @Override
        public Void execute()
        {
            if (_exception != null)
            {
                throw _exception;
            }
            _taskSubject.set(SubjectExecutionContext.currentSubject());
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

    private class SubjectRetriever implements Task<Subject, RuntimeException>
    {
        @Override
        public Subject execute()
        {
            return SubjectExecutionContext.currentSubject();
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
