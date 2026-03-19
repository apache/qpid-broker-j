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

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import javax.security.auth.Subject;

/**
 * Wrapper over {@link Subject#callAs(Subject, Callable)}.
 */
public final class SubjectExecutionContext
{
    private SubjectExecutionContext()
    {
        // utility class has private constructor
    }

    public static <T> T withSubject(final Subject subject, final Callable<T> action) throws Exception
    {
        try
        {
            return Subject.callAs(subject, action);
        }
        catch (CompletionException ce)
        {
            final Throwable cause = ce.getCause();
            if (cause == null)
            {
                throw ce;
            }
            if (cause instanceof Error err)
            {
                err.addSuppressed(ce);
                throw err;
            }
            if (cause instanceof RuntimeException re)
            {
                re.addSuppressed(ce);
                throw re;
            }
            if (cause instanceof Exception ex)
            {
                ex.addSuppressed(ce);
                throw ex;
            }
            throw ce;
        }
    }

    public static <T> T withSubjectUnchecked(final Subject subject, final Callable<T> action)
    {
        try
        {
            return Subject.callAs(subject, action);
        }
        catch (CompletionException ce)
        {
            final Throwable cause = ce.getCause();
            if (cause == null)
            {
                throw ce;
            }
            if (cause instanceof Error err)
            {
                err.addSuppressed(ce);
                throw err;
            }
            if (cause instanceof RuntimeException re)
            {
                re.addSuppressed(ce);
                throw re;
            }
            if (cause instanceof Exception ex)
            {
                ex.addSuppressed(ce);
                throw new SubjectActionException(ex);
            }
            throw ce;
        }
    }

    public static void withSubject(final Subject subject, final Runnable runnable)
    {
        try
        {
            Subject.callAs(subject, () ->
            {
                runnable.run();
                return null;
            });
        }
        catch (CompletionException ce)
        {
            final Throwable cause = ce.getCause();
            if (cause == null)
            {
                throw ce;
            }
            if (cause instanceof Error err)
            {
                err.addSuppressed(ce);
                throw err;
            }
            if (cause instanceof RuntimeException re)
            {
                re.addSuppressed(ce);
                throw re;
            }
            if (cause instanceof Exception ex)
            {
                ex.addSuppressed(ce);
                throw new SubjectActionException(ex);
            }
            throw ce;
        }
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
