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

import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.util.FutureHelper;

public class TaskExecutorImpl implements TaskExecutor
{
    private static final String TASK_EXECUTION_THREAD_NAME = "Broker-Config";
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutorImpl.class);
    private static final Cache<Set<Principal>, Subject> SUBJECT_CACHE = Caffeine.newBuilder()
            .expireAfterAccess(Duration.ofMinutes(5))
            .maximumSize(1000)
            .build();

    private final PrincipalAccessor _principalAccessor;
    private final AtomicBoolean _running = new AtomicBoolean();
    private final ImmediateIfSameThreadExecutor _wrappedExecutor = new ImmediateIfSameThreadExecutor();
    private final String _name;

    private volatile Thread _taskThread;
    private volatile ExecutorService _executor;

    public TaskExecutorImpl()
    {
        this(TASK_EXECUTION_THREAD_NAME, null);
    }

    public TaskExecutorImpl(final String name, final PrincipalAccessor principalAccessor)
    {
        _name = name;
        _principalAccessor = principalAccessor;
    }

    @Override
    public boolean isRunning()
    {
        return _running.get();
    }

    @Override
    public void start()
    {
        if (_running.compareAndSet(false, true))
        {
            LOGGER.debug("Starting task executor {}", _name);
            final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
            final ThreadFactory factory =
                    QpidByteBuffer.createQpidByteBufferTrackingThreadFactory(runnable ->
            {
                _taskThread = new TaskThread(runnable, _name, TaskExecutorImpl.this);
                return _taskThread;
            });
            _executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, workQueue, factory);
            LOGGER.debug("Task executor is started");
        }
    }

    @Override
    public void stopImmediately()
    {
        if (_running.compareAndSet(true, false))
        {
            final ExecutorService executor = _executor;
            if (executor != null)
            {
                LOGGER.debug("Stopping task executor {} immediately", _name);
                final List<Runnable> cancelledTasks = executor.shutdownNow();
                cancelledTasks.forEach(runnable -> ((RunnableWrapper) runnable).cancel());
                _executor = null;
                _taskThread = null;
                LOGGER.debug("Task executor was stopped immediately. Number of unfinished tasks: {}", cancelledTasks.size());
            }
            SUBJECT_CACHE.invalidateAll();
        }
    }

    @Override
    public void stop()
    {
        if (_running.compareAndSet(true, false))
        {
            final ExecutorService executor = _executor;
            if (executor != null)
            {
                LOGGER.debug("Stopping task executor {}", _name);
                executor.shutdown();
                _executor = null;
                _taskThread = null;
                LOGGER.debug("Task executor is stopped");
            }
            SUBJECT_CACHE.invalidateAll();
        }
    }

    @Override
    public <T, E extends Exception> CompletableFuture<T> submit(final Task<T, E> userTask) throws E
    {
        return submitWrappedTask(userTask);
    }

    private <T, E extends Exception> CompletableFuture<T> submitWrappedTask(final Task<T, E> task) throws E
    {
        checkState(task);
        if (isTaskExecutorThread())
        {
            if (LOGGER.isTraceEnabled())
            {
                LOGGER.trace("Running {} immediately", task);
            }
            final T result = task.execute();
            return CompletableFuture.completedFuture(result);
        }
        else
        {
            if (LOGGER.isTraceEnabled())
            {
                LOGGER.trace("Submitting {} to executor {}", task, _name);
            }

            final CompletableFuture<T> future = new CompletableFuture<>();
            _executor.execute(new RunnableWrapper<>(task, future));
            return future;
        }
    }

    @Override
    public void execute(final Runnable command)
    {
        if (LOGGER.isTraceEnabled())
        {
            LOGGER.trace("Running runnable {} through executor interface", command);
        }
        _wrappedExecutor.execute(command);
    }

    @Override
    public <T, E extends Exception> T run(final Task<T, E> userTask) throws CancellationException, E
    {
        return FutureHelper.<T, E>await(submitWrappedTask(userTask));
    }

    private boolean isTaskExecutorThread()
    {
        return Thread.currentThread() == _taskThread;
    }

    private void checkState(final Task<?, ?> task)
    {
        if (!_running.get())
        {
            LOGGER.error("Task executor {} is not in ACTIVE state, unable to execute : {} ", _name, task);
            throw new IllegalStateException("Task executor " + _name + " is not in ACTIVE state");
        }
    }

    private Subject getCachedSubject()
    {
        final Subject contextSubject = Subject.getSubject(AccessController.getContext());

        if (contextSubject == null)
        {
            return null;
        }

        if (_principalAccessor == null || contextSubject.getPrincipals().contains(_principalAccessor.getPrincipal()))
        {
            return contextSubject;
        }

        final Set<Principal> principals = new HashSet<>(contextSubject.getPrincipals());
        principals.add(_principalAccessor.getPrincipal());

        return SUBJECT_CACHE.get(principals, key -> createSubjectWithPrincipals(key, contextSubject));
    }

    Subject createSubjectWithPrincipals(final Set<Principal> principals, Subject subject)
    {
        return new Subject(subject.isReadOnly(), principals, subject.getPublicCredentials(), subject.getPrivateCredentials());
    }

    private class ImmediateWrapper<T, E extends Exception> extends RunnableWrapper<T, E>
    {
        final Runnable _runnable;
        final Subject _subject;

        boolean _cancelled;

        ImmediateWrapper(final Runnable runnable, final Subject subject)
        {
            super(null, null);
            _runnable = runnable;
            _subject = subject;
        }

        @Override
        public void run()
        {
            if (_cancelled)
            {
                return;
            }
            Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
            {
                _runnable.run();
                return null;
            });
        }

        void cancel()
        {
            _cancelled = true;
        }
    }

    private class RunnableWrapper<T, E extends Exception> implements Runnable
    {
        private final Task<T, E> _userTask;
        private final CompletableFuture<T> _future;
        private final Subject _contextSubject;
        private final AtomicReference<Throwable> _throwable;

        public RunnableWrapper(final Task<T, E> userWork, final CompletableFuture<T> future)
        {
            _userTask = userWork;
            _future = future;
            _contextSubject = getCachedSubject();
            _throwable = new AtomicReference<>();
        }

        public void run()
        {
            if (_future.isCancelled() || _future.isCompletedExceptionally())
            {
                return;
            }
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Performing {}", _userTask);
            }
            final T result = Subject.doAs(_contextSubject, (PrivilegedAction<T>) () ->
            {
                try
                {
                    return _userTask.execute();
                }
                catch (Throwable t)
                {
                    _throwable.set(t);
                    _future.obtrudeException(t);
                }
                return null;
            });

            final Throwable throwable = _throwable.get();
            if (throwable != null)
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("{} failed to perform successfully", _userTask);
                }
                if (throwable instanceof RuntimeException)
                {
                    throw (RuntimeException) throwable;
                }
                else if (throwable instanceof Error)
                {
                    throw (Error) throwable;
                }
                else
                {
                    throw new RuntimeException(throwable);
                }
            }

            LOGGER.debug("{} performed successfully with result: {}", _userTask, result);
            _future.complete(result);
        }

        void cancel()
        {
            _future.completeExceptionally(new CancellationException("Task was cancelled"));
        }
    }

    private class ImmediateIfSameThreadExecutor implements Executor
    {
        @Override
        public void execute(final Runnable command)
        {
            if (isTaskExecutorThread() || (_executor == null && (Thread.currentThread() instanceof TaskThread &&
                    ((TaskThread)Thread.currentThread()).getTaskExecutor() == TaskExecutorImpl.this)))
            {
                command.run();
            }
            else
            {
                _executor.execute(new ImmediateWrapper<>(command, getCachedSubject()));
            }
        }
    }

    private static class TaskThread extends Thread
    {
        private final TaskExecutorImpl _taskExecutor;

        public TaskThread(final Runnable r, final String name, final TaskExecutorImpl taskExecutor)
        {
            super(r, name);
            _taskExecutor = taskExecutor;
        }

        public TaskExecutorImpl getTaskExecutor()
        {
            return _taskExecutor;
        }
    }

    @Override
    public Factory getFactory()
    {
        return new Factory()
        {
            @Override
            public TaskExecutor newInstance()
            {
                return new TaskExecutorImpl();
            }

            @Override
            public TaskExecutor newInstance(final String name, final PrincipalAccessor principalAccessor)
            {
                return new TaskExecutorImpl(name, principalAccessor);
            }
        };
    }
}
