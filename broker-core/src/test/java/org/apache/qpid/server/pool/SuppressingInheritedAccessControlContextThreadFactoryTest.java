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
package org.apache.qpid.server.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class SuppressingInheritedAccessControlContextThreadFactoryTest extends UnitTestBase
{
    @Test
    public void testAccessControlContextIsNotInheritedByThread() throws Exception
    {
        final String principalName = getTestName();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<AccessControlContext> threadAccessControlContextCapturer = new AtomicReference<>();
        final AtomicReference<AccessControlContext> callerAccessControlContextCapturer = new AtomicReference<>();
        final Set<Principal> principals = Set.of(new Principal()
        {
            @Override
            public String getName()
            {
                return principalName;
            }

            @Override
            public String toString()
            {
                return "Principal{" + getName() + "}";
            }
        });

        final Subject subject = new Subject(false, principals, Set.of(), Set.of());

        Subject.doAs(subject, (PrivilegedAction<Void>) () ->
        {
            callerAccessControlContextCapturer.set(AccessController.getContext());
            final SuppressingInheritedAccessControlContextThreadFactory factory =
                    new SuppressingInheritedAccessControlContextThreadFactory(null, null);
            factory.newThread(() ->
            {
                threadAccessControlContextCapturer.set(AccessController.getContext());
                latch.countDown();
            }).start();
            return null;
        });

        latch.await(3, TimeUnit.SECONDS);

        final Subject callerSubject = Subject.getSubject(callerAccessControlContextCapturer.get());
        final Subject threadSubject = Subject.getSubject(threadAccessControlContextCapturer.get());

        assertEquals(callerSubject, subject, "Unexpected subject in main thread");
        assertNull(threadSubject, "Unexpected subject in executor thread");
    }
}
