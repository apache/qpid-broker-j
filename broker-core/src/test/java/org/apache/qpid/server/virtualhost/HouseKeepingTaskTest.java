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

package org.apache.qpid.server.virtualhost;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.test.utils.UnitTestBase;

public class HouseKeepingTaskTest extends UnitTestBase
{
    @Test
    public void runExecuteThrowsConnectionScopeRuntimeException()
    {
        final VirtualHost<?> virtualHost = mock(VirtualHost.class);
        final HouseKeepingTask task = new HouseKeepingTask(getTestName(), virtualHost, null)
        {
            @Override
            public void execute()
            {
                throw new ConnectionScopedRuntimeException("Test");
            }
        };
        task.run();
    }

    @Test
    public void runExecuteUsesSubject()
    {
        final VirtualHost<?> virtualHost = mock(VirtualHost.class);
        final Subject subject = new Subject();
        final AtomicReference<Subject> capturedSubject = new AtomicReference<>();

        final HouseKeepingTask task = new HouseKeepingTask(getTestName(), virtualHost, subject)
        {
            @Override
            public void execute()
            {
                capturedSubject.set(Subject.current());
            }
        };

        task.run();
        assertEquals(subject, capturedSubject.get(), "Unexpected subject");
    }
}
