/*
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

package org.apache.qpid.server.txn;

import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class AsyncCommand
{
    private final ListenableFuture<Void> _future;
    private ServerTransaction.Action _action;

    public AsyncCommand(final ListenableFuture<Void> future, final ServerTransaction.Action action)
    {
        _future = future;
        _action = action;
    }

    public void complete()
    {
        boolean interrupted = false;
        boolean success = false;
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
            success = true;
        }
        catch(ExecutionException e)
        {
            if(e.getCause() instanceof RuntimeException)
            {
                throw (RuntimeException)e.getCause();
            }
            else if(e.getCause() instanceof Error)
            {
                throw (Error) e.getCause();
            }
            else
            {
                throw new ServerScopedRuntimeException(e.getCause());
            }
        }
        finally
        {
            if(interrupted)
            {
                Thread.currentThread().interrupt();
            }
            if (success)
            {
                _action.postCommit();
            }
            else
            {
                _action.onRollback();
            }
            _action = null;
        }
    }

    public boolean isReadyForCompletion()
    {
        return _future.isDone();
    }
}
