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
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.util.FutureHelper;

public class TaskExecutorImpl implements TaskExecutor
{
    private static final String TASK_EXECUTION_THREAD_NAME = "Broker-Config";
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutorImpl.class);
    private final PrincipalAccessor _principalAccessor;

    private volatile Thread _taskThread;
    private final AtomicBoolean _running = new AtomicBoolean();
    private volatile ListeningExecutorService _executor;
    private final ImmediateIfSameThreadExecutor _wrappedExecutor = new ImmediateIfSameThreadExecutor();
    private final String _name;

    public TaskExecutorImpl()
    {
        this(TASK_EXECUTION_THREAD_NAME, null);
    }

    public TaskExecutorImpl(final String name, PrincipalAccessor principalAccessor)
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
            final java.util.concurrent.BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
            final java.util.concurrent.ThreadFactory factory = r ->
            {
                _taskThread =
                        new TaskThread(
                                r,
                                _name,
                                TaskExecutorImpl.this);
                return _taskThread;
            };
            _executor = MoreExecutors.listeningDecorator(new ThreadPoolExecutor(1,
                                                                                1,
                                                                                0L,
                                                                                TimeUnit.MILLISECONDS,
                                                                                workQueue,
                                                                                QpidByteBuffer.createQpidByteBufferTrackingThreadFactory(factory)));
            LOGGER.debug("Task executor is started");
        }
    }

    @Override
    public void stopImmediately()
    {
        if (_running.compareAndSet(true,false))
        {
            ExecutorService executor = _executor;
            if (executor != null)
            {
                LOGGER.debug("Stopping task executor {} immediately", _name);
                List<Runnable> cancelledTasks = executor.shutdownNow();
                for (Runnable runnable : cancelledTasks)
                {
                    if (runnable instanceof RunnableFuture<?>)
                    {
                        ((RunnableFuture<?>) runnable).cancel(true);
                    }
                }

                _executor = null;
                _taskThread = null;
                LOGGER.debug("Task executor was stopped immediately. Number of unfinished tasks: " + cancelledTasks.size());
            }
        }
    }

    @Override
    public void stop()
    {
        if (_running.compareAndSet(true, false))
        {
            ExecutorService executor = _executor;
            if (executor != null)
            {
                LOGGER.debug("Stopping task executor {}", _name);
                executor.shutdown();
                _executor = null;
                _taskThread = null;
                LOGGER.debug("Task executor is stopped");
            }
        }
    }

    @Override
    public <T, E extends Exception> ListenableFuture<T> submit(Task<T, E> userTask) throws E
    {
        return submitWrappedTask(new TaskLoggingWrapper<>(userTask));
    }

    private <T, E extends Exception> ListenableFuture<T> submitWrappedTask(TaskLoggingWrapper<T, E> task) throws E
    {
        checkState(task);
        if (isTaskExecutorThread())
        {
            if (LOGGER.isTraceEnabled())
            {
                LOGGER.trace("Running {} immediately", task);
            }
            T result = task.execute();
            return Futures.immediateFuture(result);
        }
        else
        {
            if (LOGGER.isTraceEnabled())
            {
                LOGGER.trace("Submitting {} to executor {}", task, _name);
            }

            return _executor.submit(new CallableWrapper<>(task));
        }
    }

    @Override
    public void execute(final Runnable command)
    {
        LOGGER.trace("Running runnable {} through executor interface", command);
        _wrappedExecutor.execute(command);
    }

    @Override
    public <T, E extends Exception> T run(Task<T, E> userTask) throws CancellationException, E
    {
        TaskLoggingWrapper<T, E> task = new TaskLoggingWrapper<>(userTask);
        return FutureHelper.<T, E>await(submitWrappedTask(task));
    }

    private boolean isTaskExecutorThread()
    {
        return Thread.currentThread() == _taskThread;
    }

    private void checkState(Task<?, ?> task)
    {
        if (!_running.get())
        {
            LOGGER.error("Task executor {} is not in ACTIVE state, unable to execute : {} ", _name, task);
            throw new IllegalStateException("Task executor " + _name + " is not in ACTIVE state");
        }
    }

    private Subject getContextSubject()
    {
        Subject contextSubject = Subject.getSubject(AccessController.getContext());
        if (contextSubject != null && _principalAccessor != null)
        {
            Principal additionalPrincipal = _principalAccessor.getPrincipal();
            Set<Principal> principals = contextSubject.getPrincipals();
            if (additionalPrincipal != null && !principals.contains(additionalPrincipal))
            {
                Set<Principal> extendedPrincipals = new HashSet<>(principals);
                extendedPrincipals.add(additionalPrincipal);
                contextSubject = new Subject(contextSubject.isReadOnly(),
                        extendedPrincipals,
                        contextSubject.getPublicCredentials(),
                        contextSubject.getPrivateCredentials());
            }
        }
        return contextSubject;
    }

    private class TaskLoggingWrapper<T, E extends Exception> implements Task<T, E>
    {
        private final Task<T,E> _task;

        public TaskLoggingWrapper(Task<T, E> task)
        {
            _task = task;
        }

        @Override
        public T execute() throws E
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Performing {}", this);
            }

            boolean success = false;
            T result = null;
            try
            {
                result = _task.execute();
                success = true;
            }
            finally
            {
                if (LOGGER.isDebugEnabled())
                {
                    if (success)
                    {
                        LOGGER.debug("{} performed successfully with result: {}", this, result);
                    } else
                    {
                        LOGGER.debug("{} failed to perform successfully", this);
                    }
                }
            }
            return result;
        }

        @Override
        public String getObject()
        {
            return _task.getObject();
        }

        @Override
        public String getAction()
        {
            return _task.getAction();
        }

        @Override
        public String getArguments()
        {
            return _task.getArguments();
        }

        @Override
        public String toString()
        {
            String arguments =  getArguments();
            if (arguments == null)
            {
                return String.format("Task['%s' on '%s']", getAction(), getObject());
            }
            return String.format("Task['%s' on '%s' with arguments '%s']", getAction(), getObject(), arguments);
        }
    }

    private class CallableWrapper<T, E extends Exception> implements Callable<T>
    {
        private final Task<T, E> _userTask;
        private final Subject _contextSubject;
        private final AtomicReference<Throwable> _throwable;

        public CallableWrapper(Task<T, E> userWork)
        {
            _userTask = userWork;
            _contextSubject = getContextSubject();
            _throwable = new AtomicReference<>();
        }

        @Override
        public T call() throws Exception
        {
            T result =  Subject.doAs(_contextSubject, new PrivilegedAction<T>()
                {
                    @Override
                    public T run()
                    {
                        try
                        {
                            return _userTask.execute();
                        }
                        catch(Throwable t)
                        {
                            _throwable.set(t);
                        }
                        return null;
                    }
                });
            Throwable t = _throwable.get();
            if (t != null)
            {
                if (t instanceof RuntimeException)
                {
                    throw (RuntimeException) t;
                }
                else if (t instanceof Error)
                {
                    throw (Error) t;
                }
                else
                {
                    throw (Exception) t;
                }
            }
            return result;
        }
    }

    private static class ImmediateFuture<T> implements Future<T>
    {
        private T _result;

        public ImmediateFuture(T result)
        {
            super();
            _result = result;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public T get()
        {
            return _result;
        }

        @Override
        public T get(long timeout, TimeUnit unit)
        {
            return get();
        }
    }

    private class ImmediateIfSameThreadExecutor implements Executor
    {

        @Override
        public void execute(final Runnable command)
        {
            if(isTaskExecutorThread()
               || (_executor == null && (Thread.currentThread() instanceof TaskThread
                   && ((TaskThread)Thread.currentThread()).getTaskExecutor() == TaskExecutorImpl.this)))
            {
                command.run();
            }
            else
            {
                final Subject subject = getContextSubject();
                _executor.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        Subject.doAs(subject, new PrivilegedAction<Void>()
                        {
                            @Override
                            public Void run()
                            {
                                command.run();
                                return null;
                            }
                        });
                    }
                });
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
            public TaskExecutor newInstance(final String name, PrincipalAccessor principalAccessor)
            {
                return new TaskExecutorImpl(name, principalAccessor);
            }
        };
    }
}
