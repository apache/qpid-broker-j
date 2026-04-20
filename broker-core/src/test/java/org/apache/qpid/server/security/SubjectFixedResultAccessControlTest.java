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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import java.security.Principal;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.PermissionedObject;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.test.utils.UnitTestBase;

public class SubjectFixedResultAccessControlTest extends UnitTestBase
{
    @Test
    public void newTokenUsesCurrentSubject()
    {
        final Subject subject = new Subject(false, Set.of((Principal) () -> getTestName()), Set.of(), Set.of());
        final AtomicReference<Subject> seenSubject = new AtomicReference<>();

        final SubjectFixedResultAccessControl accessControl =
                new SubjectFixedResultAccessControl(subj ->
                {
                    seenSubject.set(subj);
                    return Result.ALLOWED;
                }, Result.DEFER);

        SubjectExecutionContext.withSubject(subject, () ->
        {
            final SubjectFixedResultAccessControl.FixedResultSecurityToken token = accessControl.newToken();
            final Result result = accessControl.authorise(token, Operation.UPDATE, mock(PermissionedObject.class));
            assertEquals(Result.ALLOWED, result, "Unexpected authorise result");
        });

        assertSame(subject, seenSubject.get(), "Unexpected subject");
    }

    @Test
    public void newTokenUsesProvidedSubject() throws Exception
    {
        final Subject subject = new Subject(false, Set.of((Principal) () -> getTestName()), Set.of(), Set.of());
        final Subject otherSubject = new Subject();
        final AtomicReference<Subject> seenSubject = new AtomicReference<>();

        final SubjectFixedResultAccessControl accessControl =
                new SubjectFixedResultAccessControl(subj ->
                {
                    seenSubject.set(subj);
                    return Result.DENIED;
                }, Result.DEFER);

        SubjectExecutionContext.withSubject(otherSubject, () -> accessControl.newToken(subject));

        assertSame(subject, seenSubject.get(), "Unexpected subject");
    }

    @Test
    public void authoriseWithNullTokenUsesCurrentSubject()
    {
        final Subject subject = new Subject(false, Set.of((Principal) () -> getTestName()), Set.of(), Set.of());
        final AtomicReference<Subject> seenSubject = new AtomicReference<>();

        final SubjectFixedResultAccessControl accessControl =
                new SubjectFixedResultAccessControl(subj ->
                {
                    seenSubject.set(subj);
                    return Result.DENIED;
                }, Result.DEFER);

        SubjectExecutionContext.withSubject(subject, () ->
        {
            final Result result = accessControl.authorise(null, Operation.CREATE, mock(PermissionedObject.class));
            assertEquals(Result.DENIED, result, "Unexpected authorise result");
        });

        assertSame(subject, seenSubject.get(), "Unexpected subject");
    }

    @Test
    public void defaultResult()
    {
        final SubjectFixedResultAccessControl accessControl =
                new SubjectFixedResultAccessControl(subject -> Result.DEFER, Result.ALLOWED);

        assertEquals(Result.ALLOWED, accessControl.getDefault(), "Unexpected default result");
    }
}
