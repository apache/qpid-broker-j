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

import java.security.PrivilegedAction;

import javax.security.auth.Subject;

import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.security.SecurityManager;

public abstract class HouseKeepingTask implements Runnable
{
    private final String _name;
    private final Subject _subject;

    public HouseKeepingTask(VirtualHost vhost)
    {
        this(vhost, null);
    }

    public HouseKeepingTask(VirtualHost vhost, Subject subject)
    {
        _name = vhost.getName() + ":" + this.getClass().getSimpleName();
        if (subject == null)
        {
            _subject = SecurityManager.getSystemTaskSubject(_name, vhost.getPrincipal());
        }
        else
        {
            _subject = subject;
        }
    }

    final public void run()
    {
        String originalThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(_name);

        try
        {
            Subject.doAs(_subject, new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    execute();
                    return null;
                }
            });
        }
        finally
        {
            // eagerly revert the thread name to make thread dumps more meaningful if captured after task has finished
            Thread.currentThread().setName(originalThreadName);
        }
    }

    /** Execute the plugin. */
    public abstract void execute();

}
