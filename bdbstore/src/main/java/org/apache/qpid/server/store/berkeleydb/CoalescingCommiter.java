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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import org.apache.qpid.server.store.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.store.StoreException;

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
    public ListenableFuture<Void> commit(Transaction tx, boolean syncCommit)
    {
        ThreadNotifyingSettableFuture future = new ThreadNotifyingSettableFuture();
        BDBCommitFutureResult commitFuture = new BDBCommitFutureResult(_commitThread, tx, syncCommit, future);
        commitFuture.commit();
        return future;
    }

    private static final class BDBCommitFutureResult
    {
        private static final Logger LOGGER = LoggerFactory.getLogger(BDBCommitFutureResult.class);

        private final CommitThread _commitThread;
        private final Transaction _tx;
        private final boolean _syncCommit;
        private final ThreadNotifyingSettableFuture _future;

        public BDBCommitFutureResult(CommitThread commitThread,
                                     Transaction tx,
                                     boolean syncCommit,
                                     final ThreadNotifyingSettableFuture future)
        {
            _commitThread = commitThread;
            _tx = tx;
            _syncCommit = syncCommit;
            _future = future;
        }

        public void complete()
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("complete() called for transaction " + _tx);
            }
            _future.set(null);
        }

        public void abort(RuntimeException databaseException)
        {
            _future.setException(databaseException);
        }

        public void commit() throws DatabaseException
        {
            _commitThread.addJob(this, _syncCommit);

            if(!_syncCommit)
            {
                if(LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("CommitAsync was requested, returning immediately.");
                }
                return;
            }

            boolean interrupted = false;
            try
            {
                while (true)
                {
                    try
                    {
                        _future.get();
                        break;
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

            }
            catch (ExecutionException e)
            {
                if(e.getCause() instanceof RuntimeException)
                {
                    throw (RuntimeException)e.getCause();
                }
                else if(e.getCause() instanceof Error)
                {
                    throw (Error)e.getCause();
                }
                else
                {
                    throw new StoreException(e.getCause());
                }
            }
        }
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

        private final AtomicBoolean _stopped = new AtomicBoolean(false);
        private final Queue<BDBCommitFutureResult> _jobQueue = new ConcurrentLinkedQueue<BDBCommitFutureResult>();
        private final Object _lock = new Object();
        private final EnvironmentFacade _environmentFacade;

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
                            _lock.wait(1000);
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
            int size = _jobQueue.size();

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

                for(int i = 0; i < size; i++)
                {
                    BDBCommitFutureResult commit = _jobQueue.poll();
                    if (commit == null)
                    {
                        break;
                    }
                    commit.complete();
                }

            }
            catch (RuntimeException e)
            {
                try
                {
                    LOGGER.error("Exception during environment log flush", e);

                    for(int i = 0; i < size; i++)
                    {
                        BDBCommitFutureResult commit = _jobQueue.poll();
                        if (commit == null)
                        {
                            break;
                        }
                        commit.abort(e);
                    }
                }
                finally
                {
                    LOGGER.error("Closing store environment", e);

                    try
                    {
                        _environmentFacade.close();
                    }
                    catch (Exception ex)
                    {
                        LOGGER.error("Exception closing store environment", ex);
                    }
                }
            }
        }

        private boolean hasJobs()
        {
            return !_jobQueue.isEmpty();
        }

        public void addJob(BDBCommitFutureResult commit, final boolean sync)
        {
            if (_stopped.get())
            {
                throw new IllegalStateException("Commit thread is stopped");
            }
            _jobQueue.add(commit);
            if(sync)
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
                BDBCommitFutureResult commit;

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

    private final class ThreadNotifyingSettableFuture extends AbstractFuture<Void>
    {
        @Override
        public Void get(final long timeout, final TimeUnit unit)
                throws InterruptedException, TimeoutException, ExecutionException
        {
            _commitThread.explicitNotify();
            return super.get(timeout, unit);
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException
        {
            _commitThread.explicitNotify();
            return super.get();
        }

        @Override
        protected boolean set(final Void value)
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
}
