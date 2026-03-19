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

package org.apache.qpid.server.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;

class SubjectExecutionContextTest extends UnitTestBase
{
    private static final String SUBJECT_MUST_BE_NULL_OUTSIDE_CONTEXT = "Subject must be null outside context";
    private static final String SUBJECT_MUST_BE_RESTORED_AFTER_CONTEXT = "Subject must be restored after context";

    @Test
    void withSubjectRestoresPreviousSubject()
    {
        final Subject subjectA = new Subject();
        final Subject subjectB = new Subject();

        assertNull(Subject.current(), SUBJECT_MUST_BE_NULL_OUTSIDE_CONTEXT);

        SubjectExecutionContext.withSubject(subjectA, () ->
        {
            assertEquals(subjectA, Subject.current(), "Unexpected subject in outer context");

            SubjectExecutionContext.withSubject(subjectB, () ->
                    assertEquals(subjectB, Subject.current(), "Unexpected subject in inner context"));

            assertEquals(subjectA, Subject.current(), "Unexpected subject after inner context");
        });

        assertNull(Subject.current(), SUBJECT_MUST_BE_RESTORED_AFTER_CONTEXT);
    }

    @Test
    void withSubjectRunnableRestoresPreviousSubject()
    {
        final Subject subjectA = new Subject();
        final Subject subjectB = new Subject();

        assertNull(Subject.current(), SUBJECT_MUST_BE_NULL_OUTSIDE_CONTEXT);

        SubjectExecutionContext.withSubject(subjectA, () ->
        {
            assertEquals(subjectA, Subject.current(), "Unexpected subject in outer context");

            SubjectExecutionContext.withSubject(subjectB, () ->
                    assertEquals(subjectB, Subject.current(), "Unexpected subject in inner context"));

            assertEquals(subjectA, Subject.current(), "Unexpected subject after inner context");
        });

        assertNull(Subject.current(), SUBJECT_MUST_BE_RESTORED_AFTER_CONTEXT);
    }

    @Test
    void withSubjectRestoresAfterException()
    {
        final Subject subject = new Subject();
        final Exception expected = new Exception("boom");

        final Exception thrown = assertThrows(Exception.class, () ->
                SubjectExecutionContext.withSubject(subject, (Callable<Void>) () ->
                {
                    throw expected;
                }));

        assertSame(expected, thrown, "Unexpected exception");
        assertNull(Subject.current(), "Subject must be restored after exception");
    }

    @Test
    void withSubjectRunnableRestoresAfterException()
    {
        final Subject subject = new Subject();
        final RuntimeException expected = new RuntimeException("boom");

        final RuntimeException thrown = assertThrows(RuntimeException.class, () ->
                SubjectExecutionContext.withSubject(subject, (Runnable) () ->
                {
                    throw expected;
                }));

        assertSame(expected, thrown, "Unexpected exception");
        assertNull(Subject.current(), "Subject must be restored after exception");
    }

    @Test
    void withSubjectRunnableRestoresAfterNestedException()
    {
        final Subject subjectA = new Subject();
        final Subject subjectB = new Subject();
        final RuntimeException expected = new RuntimeException("boom");

        assertNull(Subject.current(), SUBJECT_MUST_BE_NULL_OUTSIDE_CONTEXT);

        SubjectExecutionContext.withSubject(subjectA, () ->
        {
            assertEquals(subjectA, Subject.current(), "Unexpected subject in outer context");

            try
            {
                SubjectExecutionContext.withSubject(subjectB, (Runnable) () ->
                {
                    assertEquals(subjectB, Subject.current(), "Unexpected subject in inner context");
                    throw expected;
                });
            }
            catch (RuntimeException thrown)
            {
                assertSame(expected, thrown, "Unexpected exception");
            }

            assertEquals(subjectA,
                    Subject.current(),
                    "Unexpected subject after inner exception");
        });

        assertNull(Subject.current(), SUBJECT_MUST_BE_RESTORED_AFTER_CONTEXT);
    }

    @Test
    @EnabledForJreRange(max = JRE.JAVA_22)
    void newThreadInheritsSubject() throws Exception
    {
        final Subject subject = new Subject();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Subject> threadSubjectCapture = new AtomicReference<>();

        SubjectExecutionContext.withSubject(subject, () ->
        {
            Thread thread = new Thread(() ->
            {
                threadSubjectCapture.set(Subject.current());
                latch.countDown();
            });
            thread.start();
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS), "Thread did not start in time");
        assertNotNull(threadSubjectCapture.get(), "Expected subject in new thread");
        assertEquals(subject, threadSubjectCapture.get(), "Expected subject in new thread be the same as in caller thread");
    }

    @Test
    @EnabledForJreRange(min = JRE.JAVA_23)
    void newThreadDoesNotInheritsSubject() throws Exception
    {
        final Subject subject = new Subject();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Subject> threadSubjectCapture = new AtomicReference<>();

        SubjectExecutionContext.withSubject(subject, () ->
        {
            Thread thread = new Thread(() ->
            {
                threadSubjectCapture.set(Subject.current());
                latch.countDown();
            });
            thread.start();
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS), "Thread did not start in time");
        assertNull(threadSubjectCapture.get(), "Unexpected subject in new thread");
    }

    @Test
    void withSubjectUncheckedWrapsCheckedException()
    {
        final Subject subject = new Subject();
        final Exception expected = new Exception("boom");

        final SubjectActionException thrown = assertThrows(SubjectActionException.class, () ->
                SubjectExecutionContext.withSubjectUnchecked(subject, (Callable<Void>) () ->
                {
                    throw expected;
                }));

        assertSame(expected, thrown.getCause(), "Unexpected wrapped exception");
        assertNull(Subject.current(), "Subject must be restored after exception");
    }

    @Test
    void withSubjectUncheckedRethrowsRuntimeException()
    {
        final Subject subject = new Subject();
        final RuntimeException expected = new RuntimeException("boom");

        final RuntimeException thrown = assertThrows(RuntimeException.class, () ->
                SubjectExecutionContext.withSubjectUnchecked(subject, (Callable<Void>) () ->
                {
                    throw expected;
                }));

        assertSame(expected, thrown, "Unexpected exception");
        assertNull(Subject.current(), "Subject must be restored after exception");
    }

    @Test
    void unwrapSubjectActionException()
    {
        final Exception cause = new Exception("boom");
        final SubjectActionException wrapped = new SubjectActionException(cause);

        final Throwable unwrapped = SubjectExecutionContext.unwrapSubjectActionException(wrapped);

        assertSame(cause, unwrapped, "Unexpected unwrapped exception");
    }

    @Test
    void unwrapSubjectActionExceptionDesiredType()
    {
        final IllegalStateException cause = new IllegalStateException("boom");
        final SubjectActionException wrapped = new SubjectActionException(cause);

        final IllegalStateException unwrapped = SubjectExecutionContext.unwrapSubjectActionException(
                wrapped,
                IllegalStateException.class,
                throwable -> new IllegalStateException("fallback", throwable));

        assertSame(cause, unwrapped, "Unexpected unwrapped exception");
    }

    @Test
    void unwrapSubjectActionExceptionDesiredTypeMismatchUsesFallback()
    {
        final IllegalStateException cause = new IllegalStateException("boom");
        final SubjectActionException wrapped = new SubjectActionException(cause);

        final IllegalArgumentException fallback = new IllegalArgumentException("fallback", wrapped);

        final IllegalArgumentException unwrapped = SubjectExecutionContext.unwrapSubjectActionException(
                wrapped,
                IllegalArgumentException.class,
                throwable -> fallback);

        assertSame(fallback, unwrapped, "Unexpected unwrapped exception");
    }
}
