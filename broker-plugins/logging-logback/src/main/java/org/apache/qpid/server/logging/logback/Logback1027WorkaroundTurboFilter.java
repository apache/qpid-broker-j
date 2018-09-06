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
 *
 */

package org.apache.qpid.server.logging.logback;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import org.slf4j.Marker;

public class Logback1027WorkaroundTurboFilter extends TurboFilter
{
    @Override
    public FilterReply decide(final Marker marker,
                              final Logger logger,
                              final Level level,
                              final String format,
                              final Object[] params,
                              final Throwable t)
    {
        if (t != null && hasRecursiveThrowableReference(t, null))
        {
            final int locationAwareLoggerInteger = Level.toLocationAwareLoggerInteger(level);
            logger.log(marker, logger.getName(), locationAwareLoggerInteger, format, params, new StringifiedException(t));
            return FilterReply.DENY;
        }

        return FilterReply.NEUTRAL;
    }

    private boolean hasRecursiveThrowableReference(Throwable t, Set<Throwable> seen)
    {
        if (t == null)
        {
            return false;
        }

        if (seen == null)
        {
            seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        }

        if (!seen.add(t))
        {
            return true;
        }
        else
        {
            final Throwable[] allSuppressed = t.getSuppressed();
            int allSuppressedLength = allSuppressed.length;
            if (allSuppressedLength > 0)
            {
                Set<Throwable> seenCopy = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>(seen.size()));
                seenCopy.addAll(seen);
                for (int i = 0; i < allSuppressedLength; ++i)
                {
                    if (hasRecursiveThrowableReference(allSuppressed[i], seenCopy))
                    {
                        return true;
                    }
                }
            }
            if (hasRecursiveThrowableReference(t.getCause(), seen))
            {
                return true;
            }
        }
        return false;
    }

    static class StringifiedException extends RuntimeException
    {
        public StringifiedException(final Throwable t)
        {
            super(stringifyStacktrace(t));
        }

        private static String stringifyStacktrace(final Throwable e)
        {
            try (StringWriter sw = new StringWriter();
                 PrintWriter pw = new PrintWriter(sw))
            {
                e.printStackTrace(pw);
                pw.println("End of stringified Stacktrace");
                return sw.toString();
            }
            catch (IOException e1)
            {
                throw new RuntimeException("Unexpected exception stringifying stacktrace", e1);
            }
        }
    }

}
