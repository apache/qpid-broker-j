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

import static org.mockito.Mockito.mock;

import java.security.AccessControlContext;
import java.security.AccessController;

import org.junit.Test;

import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.test.utils.UnitTestBase;

public class HouseKeepingTaskTest extends UnitTestBase
{
    @Test
    public void runExecuteThrowsConnectionScopeRuntimeException()
    {
        final VirtualHost virualHost = mock(VirtualHost.class);
        final AccessControlContext context = AccessController.getContext();
        final HouseKeepingTask task = new HouseKeepingTask(getTestName(), virualHost, context)
        {
            @Override
            public void execute()
            {
                throw new ConnectionScopedRuntimeException("Test");
            }
        };

        task.run();
    }
}
