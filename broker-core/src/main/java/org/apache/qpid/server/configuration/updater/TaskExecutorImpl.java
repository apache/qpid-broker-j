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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.util.FutureHelper;

public class TaskExecutorImpl implements TaskExecutor
{
    private static final String TASK_EXECUTION_THREAD_NAME = "Broker-Config";
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutorImpl.class);

    private final PrincipalAccessor _principalAccessor;
    private final AtomicBoolean _running = new AtomicBoolean();
    private final String _name;

    private volatile TaskThread _taskThread;
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
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Starting task executor {}", _name);
            }
            final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
            final ThreadFactory factory =
                    QpidByteBuffer.createQpidByteBufferTrackingThreadFactory(runnable ->
            {
                final TaskThread taskThread = new TaskThread(runnable, _name, TaskExecutorImpl.this);
                taskThread.setUncaughtExceptionHandler((thread, throwable) ->
                        LOGGER.error("Uncaught exception in task thread", throwable));
                _taskThread = taskThread;
                return taskThread;
            });
            _executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, workQueue, factory);
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Task executor is started");
            }
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
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Stopping task executor {} immediately", _name);
                }
                final List<Runnable> cancelledTasks = executor.shutdownNow();
                cancelledTasks.forEach(runnable ->
                {
                    if (runnable instanceof RunnableWrapper<?, ?> runnableWrapper)
                    {
                        runnableWrapper.cancel();
                    }
                });
                _executor = null;
                _taskThread = null;
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Task executor was stopped immediately. Number of unfinished tasks: {}", cancelledTasks.size());
                }
            }
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
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Stopping task executor {}", _name);
                }
                executor.shutdown();
                _executor = null;
                _taskThread = null;
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Task executor is stopped");
                }
            }
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

        if (LOGGER.isTraceEnabled())
        {
            LOGGER.trace("Submitting {} to executor {}", task, _name);
        }

        final CompletableFuture<T> future = new CompletableFuture<>();
        _executor.execute(new RunnableWrapper<>(task, future, effectiveSubject()));
        return future;
    }

    @Override
    public void execute(final Runnable command)
    {
        if (LOGGER.isTraceEnabled())
        {
            LOGGER.trace("Running runnable {} through executor interface", command);
        }
        if (isTaskExecutorThread() || (_executor == null && (Thread.currentThread() instanceof TaskThread &&
                ((TaskThread)Thread.currentThread()).getTaskExecutor() == TaskExecutorImpl.this)))
        {
            command.run();
        }
        else
        {
            _executor.execute(new RunnableWrapper<>(command, effectiveSubject()));
        }
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

    private Subject effectiveSubject()
    {
        final Subject contextSubject = Subject.getSubject(AccessController.getContext());

        if (contextSubject == null)
        {
            return null;
        }

        final Principal accessorPrincipal = _principalAccessor == null ? null : _principalAccessor.getPrincipal();

        if (accessorPrincipal == null || contextSubject.getPrincipals().contains(accessorPrincipal))
        {
            return contextSubject;
        }

        final Set<Principal> principals = new HashSet<>(contextSubject.getPrincipals());
        principals.add(accessorPrincipal);

        return new Subject(contextSubject.isReadOnly(), principals, contextSubject.getPublicCredentials(), contextSubject.getPrivateCredentials());
    }

    private static class RunnableWrapper<T, E extends Exception> implements Runnable
    {
        private final Task<T, E> _userTask;
        private final CompletableFuture<T> _future;

        private Runnable _runnable;
        private Throwable _throwable;

        private final Subject _contextSubject;

        private RunnableWrapper(final Task<T, E> userWork,
                                final CompletableFuture<T> future,
                                final Subject subject)
        {
            _userTask = userWork;
            _future = future;
            _contextSubject = subject;
        }

        private RunnableWrapper(final Runnable runnable, final Subject contextSubject)
        {
            _runnable = runnable;
            _contextSubject = contextSubject;
            _userTask = null;
            _future = null;
        }

        @Override
        public void run()
        {
            if (_runnable != null)
            {
                _runnable.run();
                return;
            }
            if (_future.isCancelled() || _future.isCompletedExceptionally())
            {
                return;
            }
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Performing {}", this);
            }
            final T result = Subject.doAs(_contextSubject, (PrivilegedAction<T>) () ->
            {
                try
                {
                    return _userTask.execute();
                }
                catch (Throwable throwable)
                {
                    _throwable = throwable;
                    _future.obtrudeException(throwable);
                }
                return null;
            });

            final Throwable throwable = _throwable;
            if (throwable != null)
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("{} failed to perform successfully", this);
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
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("{} performed successfully with result: {}", this, result);
            }
            _future.complete(result);
        }

        void cancel()
        {
            if (_future != null)
            {
                _future.completeExceptionally(new CancellationException("Task was cancelled"));
            }
        }

        @Override
        public String toString()
        {
            final String arguments = _userTask.getArguments();
            if (arguments == null)
            {
                return "Task['%s' on '%s']".formatted(_userTask.getAction(), _userTask.getObject());
            }
            return "Task['%s' on '%s' with arguments '%s']".formatted(_userTask.getAction(), _userTask.getObject(), arguments);
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
