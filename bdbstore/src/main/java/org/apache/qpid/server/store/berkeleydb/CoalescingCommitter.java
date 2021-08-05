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
package org.apache.qpid.server.store.berkeleydb;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.sleepycat.je.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.store.berkeleydb.replication.ReplicatedEnvironmentConfiguration;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost;

public class CoalescingCommitter implements Committer
{
    private static final String BATCHING_TYPE = "batching";
    private final CommitThread _commitThread;
    private final boolean _isBatching;

    public CoalescingCommitter(
            StandardEnvironmentConfiguration configuration,
            EnvironmentFacade environmentFacade)
    {
        this(configuration.getName(), configuration, environmentFacade);
    }

    public CoalescingCommitter(
            ReplicatedEnvironmentConfiguration configuration,
            EnvironmentFacade environmentFacade)
    {
        this(configuration.getGroupName(), configuration, environmentFacade);
    }

    public CoalescingCommitter(
            String name,
            StandardEnvironmentConfiguration configuration,
            EnvironmentFacade environmentFacade)
    {
        this(
                name,
                configuration.getFacadeParameter(
                        Integer.class,
                        BDBVirtualHost.QPID_BROKER_BDB_COMMITER_NOTIFY_THRESHOLD,
                        BDBVirtualHost.DEFAULT_QPID_BROKER_BDB_COMMITER_NOTIFY_THRESHOLD),
                configuration.getFacadeParameter(
                        Long.class,
                        BDBVirtualHost.QPID_BROKER_BDB_COMMITER_WAIT_TIMEOUT,
                        BDBVirtualHost.DEFAULT_QPID_BROKER_BDB_COMMITER_WAIT_TIMEOUT),
                BATCHING_TYPE.equals(configuration.getFacadeParameter(
                        String.class,
                        BDBVirtualHost.QPID_BROKER_BDB_COMMITER_TYPE,
                        BDBVirtualHost.DEFAULT_QPID_BROKER_BDB_COMMITER_TYPE)),
                environmentFacade);
    }

    public CoalescingCommitter(
            String name,
            int commiterNotifyThreshold,
            long commiterNotifyTimeout,
            boolean isBatching,
            EnvironmentFacade environmentFacade)
    {
        _isBatching = isBatching;
        _commitThread = new CommitThread(
                "Commit-Thread-" + name,
                commiterNotifyThreshold,
                commiterNotifyTimeout,
                isBatching,
                environmentFacade);
    }

    @Override
    public void start()
    {
        _commitThread.start();
    }

    @Override
    public void stop()
    {
        _commitThread.close();
        if (Thread.currentThread() != _commitThread)
        {
            try
            {
                _commitThread.join();
            }
            catch (InterruptedException ie)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Commit thread has not shutdown", ie);
            }
        }
    }

    @Override
    public void commit(Transaction tx, boolean syncCommit)
    {
        if(syncCommit)
        {
            SynchronousCommitThreadJob job = new SynchronousCommitThreadJob();
            _commitThread.addJob(job, true);
            job.awaitCompletion();
        }

    }

    @Override
    public <X> ListenableFuture<X> commitAsync(Transaction tx, X val)
    {
        final ThreadNotifyingSettableFuture<X> future = new ThreadNotifyingSettableFuture<>();
        if (_isBatching)
        {
            future.set(val);
        }
        final BDBCommitFutureResult<X> commitFuture = new BDBCommitFutureResult<>(val, future);
        _commitThread.addJob(commitFuture, false);
        return future;
    }

    private static final class BDBCommitFutureResult<X> implements CommitThreadJob
    {
        private final X _value;
        private final ThreadNotifyingSettableFuture<X> _future;

        public BDBCommitFutureResult(X value, final ThreadNotifyingSettableFuture<X> future)
        {
            _value = value;
            _future = future;
        }

        @Override
        public void complete()
        {
            _future.set(_value);
        }

        @Override
        public void abort(RuntimeException databaseException)
        {
            _future.setException(databaseException);
        }
    }

    private interface CommitThreadJob
    {
        void complete();

        void abort(RuntimeException e);
    }

    /**
     * Implements a thread which batches and commits a queue of {@link CoalescingCommitter.BDBCommitFutureResult} operations. The commit operations
     * themselves are responsible for adding themselves to the queue and waiting for the commit to happen before
     * continuing, but it is the responsibility of this thread to tell the commit operations when they have been
     * completed by calling back on their {@link CoalescingCommitter.BDBCommitFutureResult#complete()} and {@link CoalescingCommitter.BDBCommitFutureResult#abort} methods.
     *
     * <p/><table id="crc"><caption>CRC Card</caption> <tr><th> Responsibilities <th> Collaborations </table>
     */
    private static class CommitThread extends Thread
    {
        private static final Logger LOGGER = LoggerFactory.getLogger(CommitThread.class);

        private final int _jobQueueNotifyThreshold;
        private final long _commiterWaitTimeout;
        private final AtomicBoolean _stopped = new AtomicBoolean(false);
        private final Queue<CommitThreadJob> _jobQueue = new ConcurrentLinkedQueue<>();
        private final Object _lock = new Object();
        private final EnvironmentFacade _environmentFacade;
        private final boolean _isBatching;
        private final List<CommitThreadJob> _inProcessJobs;

        public CommitThread(
                String name,
                int commiterNotifyThreshold,
                long commiterWaitTimeout,
                boolean isBatching,
                EnvironmentFacade environmentFacade)
        {
            super(name);
            _jobQueueNotifyThreshold = commiterNotifyThreshold;
            _commiterWaitTimeout = commiterWaitTimeout;
            _isBatching = isBatching;
            _environmentFacade = environmentFacade;
            _inProcessJobs = new ArrayList<>(_jobQueueNotifyThreshold);
        }

        public void explicitNotify()
        {
            synchronized (_lock)
            {
                _lock.notifyAll();
            }
        }

        @Override
        public void run()
        {
            while (!_stopped.get())
            {
                synchronized (_lock)
                {
                    while (!_stopped.get() && _jobQueue.isEmpty())
                    {
                        try
                        {
                            // Periodically wake up and check, just in case we
                            // missed a notification. Don't want to lock the broker hard.
                            _lock.wait(_commiterWaitTimeout);
                        }
                        catch (InterruptedException e)
                        {
                            // ignore
                        }
                    }
                }
                if (_isBatching)
                {
                    synchronized (_lock)
                    {
                        processJobs();
                    }
                }
                else
                {
                    processJobs();
                }
            }
        }

        private void processJobs()
        {
            CommitThreadJob job;
            while((job = _jobQueue.poll()) != null)
            {
                _inProcessJobs.add(job);
            }

            int completedJobsIndex = 0;
            try
            {
                long startTime = 0;
                if(LOGGER.isDebugEnabled())
                {
                    startTime = System.currentTimeMillis();
                }

                _environmentFacade.flushLog();

                if(LOGGER.isDebugEnabled())
                {
                    final long duration = System.currentTimeMillis() - startTime;
                    LOGGER.debug("flushing log completed in {} ms, {} records flushed", duration, _inProcessJobs.size());
                }

                if (!_isBatching)
                {
                    while (completedJobsIndex < _inProcessJobs.size())
                    {
                        _inProcessJobs.get(completedJobsIndex).complete();
                        completedJobsIndex++;
                    }
                }

            }
            catch (RuntimeException e)
            {
                try
                {
                    LOGGER.error("Exception during environment log flush", e);

                    for(; completedJobsIndex < _inProcessJobs.size(); completedJobsIndex++)
                    {
                        final CommitThreadJob commit = _inProcessJobs.get(completedJobsIndex);
                        commit.abort(e);
                    }
                }
                finally
                {
                    _environmentFacade.flushLogFailed(e);
                }
            }
            finally
            {
                _inProcessJobs.clear();
            }
        }

        public void addJob(CommitThreadJob commit, final boolean sync)
        {
            if (_stopped.get())
            {
                throw new IllegalStateException("Commit thread is stopped");
            }
            if (_isBatching)
            {
                addBatchingJob(commit, sync);
            }
            else
            {
                addCoalescingJob(commit, sync);
            }
        }

        private void addBatchingJob(CommitThreadJob commit, boolean sync)
        {
            synchronized (_lock)
            {
                _jobQueue.add(commit);
                if (sync || _jobQueue.size() >= _jobQueueNotifyThreshold)
                {
                    _lock.notifyAll();
                }
            }
        }

        private void addCoalescingJob(CommitThreadJob commit, boolean sync)
        {
            _jobQueue.add(commit);
            if (sync || _jobQueue.size() >= _jobQueueNotifyThreshold)
            {
                synchronized (_lock)
                {
                    _lock.notifyAll();
                }
            }
        }

        public void close()
        {
            synchronized (_lock)
            {
                _stopped.set(true);
                CommitThreadJob commit;

                try
                {
                    _environmentFacade.flushLog();
                    while ((commit = _jobQueue.poll()) != null)
                    {
                        commit.complete();
                    }
                }
                catch (RuntimeException flushException)
                {
                    RuntimeException e = new RuntimeException("Commit thread has been closed, transaction aborted");
                    int abortedCommits = 0;
                    while ((commit = _jobQueue.poll()) != null)
                    {
                        abortedCommits++;
                        commit.abort(e);
                    }
                    if (LOGGER.isDebugEnabled() && abortedCommits > 0)
                    {
                        LOGGER.debug(abortedCommits + " commit(s) were aborted during close.");
                    }
                }

                _lock.notifyAll();
            }
        }
    }

    private final class ThreadNotifyingSettableFuture<X> extends AbstractFuture<X>
    {
        @Override
        public X get(final long timeout, final TimeUnit unit)
                throws InterruptedException, TimeoutException, ExecutionException
        {
            if (!isDone())
            {
                _commitThread.explicitNotify();
            }
            return super.get(timeout, unit);
        }

        @Override
        public X get() throws InterruptedException, ExecutionException
        {
            if (!isDone())
            {
                _commitThread.explicitNotify();
            }
            return super.get();
        }

        @Override
        protected boolean set(final X value)
        {
            return super.set(value);
        }

        @Override
        protected boolean setException(final Throwable throwable)
        {
            return super.setException(throwable);
        }

        @Override
        public void addListener(final Runnable listener, final Executor exec)
        {
            super.addListener(listener, exec);
            _commitThread.explicitNotify();
        }
    }

    private static class SynchronousCommitThreadJob implements CommitThreadJob
    {
        private boolean _done;
        private RuntimeException _exception;

        @Override
        public synchronized void complete()
        {
            _done = true;
            notifyAll();
        }

        @Override
        public synchronized void abort(final RuntimeException e)
        {
            _done = true;
            _exception = e;
            notifyAll();
        }


        public synchronized void awaitCompletion()
        {
            boolean interrupted = false;
            while (!_done)
            {
                try
                {
                    wait();
                }
                catch (InterruptedException e)
                {
                    interrupted = true;
                }
            }
            if (interrupted)
            {
                Thread.currentThread().interrupt();
            }
            if (_exception != null)
            {
                throw _exception;
            }
        }

    }
}
