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

public class CoalescingCommiter implements Committer
{
    private final CommitThread _commitThread;

    public CoalescingCommiter(String name, EnvironmentFacade environmentFacade)
    {
        _commitThread = new CommitThread("Commit-Thread-" + name, environmentFacade);
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
        ThreadNotifyingSettableFuture<X> future = new ThreadNotifyingSettableFuture<X>();
        BDBCommitFutureResult<X> commitFuture = new BDBCommitFutureResult<X>(val, future);
        _commitThread.addJob(commitFuture, false);
        return future;
    }


    private static final class BDBCommitFutureResult<X> implements CommitThreadJob
    {
        private final X _value;
        private final ThreadNotifyingSettableFuture<X> _future;

        public BDBCommitFutureResult(X value,
                                     final ThreadNotifyingSettableFuture<X> future)
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
     * Implements a thread which batches and commits a queue of {@link org.apache.qpid.server.store.berkeleydb.CoalescingCommiter.BDBCommitFutureResult} operations. The commit operations
     * themselves are responsible for adding themselves to the queue and waiting for the commit to happen before
     * continuing, but it is the responsibility of this thread to tell the commit operations when they have been
     * completed by calling back on their {@link org.apache.qpid.server.store.berkeleydb.CoalescingCommiter.BDBCommitFutureResult#complete()} and {@link org.apache.qpid.server.store.berkeleydb.CoalescingCommiter.BDBCommitFutureResult#abort} methods.
     *
     * <p/><table id="crc"><caption>CRC Card</caption> <tr><th> Responsibilities <th> Collaborations </table>
     */
    private static class CommitThread extends Thread
    {
        private static final Logger LOGGER = LoggerFactory.getLogger(CommitThread.class);
        private static final int JOB_QUEUE_NOTIFY_THRESHOLD = 8;

        private final AtomicBoolean _stopped = new AtomicBoolean(false);
        private final Queue<CommitThreadJob> _jobQueue = new ConcurrentLinkedQueue<>();
        private final Object _lock = new Object();
        private final EnvironmentFacade _environmentFacade;

        private final List<CommitThreadJob> _inProcessJobs = new ArrayList<>(256);

        public CommitThread(String name, EnvironmentFacade environmentFacade)
        {
            super(name);
            _environmentFacade = environmentFacade;
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
                    while (!_stopped.get() && !hasJobs())
                    {
                        try
                        {
                            // Periodically wake up and check, just in case we
                            // missed a notification. Don't want to lock the broker hard.
                            _lock.wait(500);
                        }
                        catch (InterruptedException e)
                        {
                        }
                    }
                }
                processJobs();
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
                    long duration = System.currentTimeMillis() - startTime;
                    LOGGER.debug("flushLog completed in " + duration  + " ms");
                }

                while(completedJobsIndex < _inProcessJobs.size())
                {
                    _inProcessJobs.get(completedJobsIndex).complete();
                    completedJobsIndex++;
                }

            }
            catch (RuntimeException e)
            {
                try
                {
                    LOGGER.error("Exception during environment log flush", e);

                    for(; completedJobsIndex < _inProcessJobs.size(); completedJobsIndex++)
                    {
                        CommitThreadJob commit = _inProcessJobs.get(completedJobsIndex);
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

        private boolean hasJobs()
        {
            return !_jobQueue.isEmpty();
        }

        public void addJob(CommitThreadJob commit, final boolean sync)
        {
            if (_stopped.get())
            {
                throw new IllegalStateException("Commit thread is stopped");
            }
            _jobQueue.add(commit);
            if(sync || _jobQueue.size() >= JOB_QUEUE_NOTIFY_THRESHOLD)
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
                catch(RuntimeException flushException)
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
            if(!isDone())
            {
                _commitThread.explicitNotify();
            }
            return super.get(timeout, unit);
        }

        @Override
        public X get() throws InterruptedException, ExecutionException
        {
            if(!isDone())
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

    private class SynchronousCommitThreadJob implements CommitThreadJob
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
            while(!_done)
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
            if(interrupted)
            {
                Thread.currentThread().interrupt();
            }
            if(_exception != null)
            {
                throw _exception;
            }
        }

    }
}
