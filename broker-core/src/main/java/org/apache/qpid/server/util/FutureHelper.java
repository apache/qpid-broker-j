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
 */

package org.apache.qpid.server.util;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.qpid.server.model.OperationTimeoutException;

public class FutureHelper
{
    public static <T, E extends Exception> T await(Future<T> future, long timeout, TimeUnit timeUnit)
            throws OperationTimeoutException, CancellationException, E
    {
        try
        {
            if (timeout > 0)
            {
                return future.get(timeout, timeUnit);
            }
            else
            {
                return future.get();
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new ServerScopedRuntimeException("Future execution was interrupted", e);
        }
        catch (ExecutionException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException)
            {
                throw (RuntimeException) cause;
            }
            else if (cause instanceof Error)
            {
                throw (Error) cause;
            }
            else
            {
                try
                {
                    throw (E) cause;
                }
                catch (ClassCastException cce)
                {
                    throw new ServerScopedRuntimeException("Future failed", cause);
                }
            }
        }
        catch (TimeoutException e)
        {
            throw new OperationTimeoutException(e);
        }
    }

    public static <T, E extends Exception> T await(Future<T> future)
            throws OperationTimeoutException, CancellationException, E
    {
        return FutureHelper.<T, E>await(future, 0, null);
    }

}
