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

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import javax.security.auth.Subject;

/**
 * Java 17-23 implementation backed by {@link Subject#getSubject(AccessControlContext)} ()} and {@link Subject#doAs(Subject, PrivilegedAction)}}.
 * <br>
 * Provides the same API surface as the Java 24+ implementation, but relies on the deprecated Security manager APIs.
 */
public final class SubjectExecutionContext
{
    private SubjectExecutionContext()
    {
        // utility class has private constructor
    }

    public static Subject currentSubject()
    {
        final AccessControlContext accessControlContext = AccessController.getContext();
        if (accessControlContext != null)
        {
            return Subject.getSubject(accessControlContext);
        }
        return null;
    }

    public static <T> T withSubject(final Subject subject, final Callable<T> action) throws Exception
    {
        try
        {
            return Subject.doAs(subject, (PrivilegedExceptionAction<T>) action::call);
        }
        catch (PrivilegedActionException pae)
        {
            final Throwable cause = pae.getCause();
            if (cause == null)
            {
                throw pae;
            }
            if (cause instanceof Error err)
            {
                err.addSuppressed(pae);
                throw err;
            }
            if (cause instanceof RuntimeException re)
            {
                re.addSuppressed(pae);
                throw re;
            }
            if (cause instanceof Exception ex)
            {
                ex.addSuppressed(pae);
                throw ex;
            }
            throw pae;
        }
    }

    public static <T> T withSubjectUnchecked(final Subject subject, final Callable<T> action)
    {
        try
        {
            return Subject.doAs(subject, (PrivilegedExceptionAction<T>) action::call);
        }
        catch (PrivilegedActionException pae)
        {
            final Throwable cause = pae.getCause();
            if (cause == null)
            {
                throw new CompletionException(pae);
            }
            if (cause instanceof Error err)
            {
                err.addSuppressed(pae);
                throw err;
            }
            if (cause instanceof RuntimeException re)
            {
                re.addSuppressed(pae);
                throw re;
            }
            if (cause instanceof Exception ex)
            {
                ex.addSuppressed(pae);
                throw new SubjectActionException(ex);
            }
            throw new CompletionException(pae);
        }
    }

    public static void withSubject(final Subject subject, final Runnable runnable)
    {
        Subject.doAs(subject, (PrivilegedAction<Object>) () ->
        {
            runnable.run();
            return null;
        });
    }

    public static Throwable unwrapSubjectActionException(final Throwable throwable)
    {
        return ((throwable instanceof SubjectActionException sae && sae.getCause() != null)
                ? sae.getCause() : throwable);
    }

    public static <T extends Exception> T unwrapSubjectActionException(final Throwable throwable,
                                                                       final Class<T> desiredType,
                                                                       final Function<Throwable, T> function)
    {
        if (desiredType.isInstance(throwable))
        {
            return desiredType.cast(throwable);
        }

        final Throwable cause = throwable.getCause();

        if (desiredType.isInstance(cause))
        {
            return desiredType.cast(cause);
        }

        return function.apply(throwable);
    }
}
