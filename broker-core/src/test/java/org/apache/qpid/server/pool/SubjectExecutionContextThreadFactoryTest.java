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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.Principal;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.security.SubjectExecutionContext;
import org.apache.qpid.test.utils.UnitTestBase;

public class SubjectExecutionContextThreadFactoryTest extends UnitTestBase
{
    @Test
    public void testThreadFactoryUsesNullSubjectByDefault() throws Exception
    {
        final String principalName = getTestName();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Subject> threadSubjectCapture = new AtomicReference<>();
        final AtomicReference<Subject> callerSubjectCapture = new AtomicReference<>();
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

        SubjectExecutionContext.withSubject(subject, () ->
        {
            callerSubjectCapture.set(SubjectExecutionContext.currentSubject());
            final SubjectExecutionContextThreadFactory factory =
                    new SubjectExecutionContextThreadFactory(null, null);
            factory.newThread(() ->
            {
                threadSubjectCapture.set(SubjectExecutionContext.currentSubject());
                latch.countDown();
            }).start();
        });

        latch.await(3, TimeUnit.SECONDS);

        final Subject callerSubject = callerSubjectCapture.get();
        final Subject threadSubject = threadSubjectCapture.get();

        assertEquals(callerSubject, subject, "Unexpected subject in main thread");
        assertNull(threadSubject, "Unexpected subject in executor thread");
    }

    @Test
    public void testFactorySubjectOverridesCallerSubject() throws Exception
    {
        final String callerPrincipalName = getTestName() + "-caller";
        final String factoryPrincipalName = getTestName() + "-factory";

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Subject> threadSubjectCapture = new AtomicReference<>();

        final Subject callerSubject = new Subject(false, Set.of(() -> callerPrincipalName), Set.of(), Set.of());

        final Subject factorySubject = new Subject(false, Set.of(() -> factoryPrincipalName), Set.of(), Set.of());

        SubjectExecutionContext.withSubject(callerSubject, () ->
        {
            final SubjectExecutionContextThreadFactory factory =
                    new SubjectExecutionContextThreadFactory(null, factorySubject);
            factory.newThread(() ->
            {
                threadSubjectCapture.set(SubjectExecutionContext.currentSubject());
                latch.countDown();
            }).start();
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS), "Thread did not start in time");
        assertEquals(factorySubject, threadSubjectCapture.get(), "Unexpected subject in executor thread");
    }
}
