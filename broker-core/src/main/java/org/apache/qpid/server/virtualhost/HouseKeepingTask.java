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
package org.apache.qpid.server.virtualhost;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

public abstract class HouseKeepingTask implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HouseKeepingTask.class);
    private final String _name;
    private final AccessControlContext _accessControlContext;
    private ScheduledFuture<?> _future;

    public HouseKeepingTask(String name, VirtualHost vhost, AccessControlContext context)
    {
        _name = name == null ? vhost.getName() + ":" + this.getClass().getSimpleName() : name;
        _accessControlContext = context;

    }

    @Override
    final public void run()
    {
        String originalThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(_name);

        try
        {
            AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    try
                    {
                        execute();
                    }
                    catch (ConnectionScopedRuntimeException e)
                    {
                        LOGGER.warn("Execution of housekeeping task failed", e);
                    }
                    return null;
                }
            }, _accessControlContext);
        }
        finally
        {
            // eagerly revert the thread name to make thread dumps more meaningful if captured after task has finished
            Thread.currentThread().setName(originalThreadName);
        }
    }

    /** Execute the plugin. */
    public abstract void execute();

    void setFuture(final ScheduledFuture<?> future)
    {
        _future = future;
    }

    public synchronized void cancel()
    {
        if(_future != null)
        {
            _future.cancel(false);
            _future = null;
        }
    }
}
