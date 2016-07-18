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

import java.security.Principal;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;

import com.google.common.util.concurrent.ListenableFuture;

public interface TaskExecutor extends Executor
{
    interface Factory
    {
        TaskExecutor newInstance();
        TaskExecutor newInstance(String name, PrincipalAccessor principalAccessor);
    }

    interface PrincipalAccessor
    {
        Principal getPrincipal();
    }

    boolean isRunning();

    void start();

    void stopImmediately();

    void stop();

    <T, E extends Exception> T run(Task<T, E> task) throws CancellationException, E;

    <T, E extends Exception> ListenableFuture<T> submit(Task<T, E> task) throws CancellationException, E;

    Factory getFactory();
}
