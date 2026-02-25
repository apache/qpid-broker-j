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
package org.apache.qpid.server.query.engine.exception;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.qpid.server.query.engine.parsing.ParseException;

/**
 * Exception thrown during query parsing
 */
public class QueryParsingException extends QueryEngineException
{
    private String message;

    private static final String INVALID_DATEPART_MARKER = " Was expecting one of: \"DAY\" ... \"HOUR\" ... "
        + "\"MILLISECOND\" ... \"MINUTE\" ... \"MONTH\" ... \"SECOND\" ... \"WEEK\" ... \"YEAR\"";

    protected QueryParsingException()
    {
        super();
    }

    protected QueryParsingException(final String message)
    {
        super(message);
    }

    public QueryParsingException(final Throwable throwable)
    {
        initCause(throwable);
        if (throwable instanceof ParseException)
        {
            final ParseException parseException = (ParseException) throwable;

            if ("select".equalsIgnoreCase(parseException.currentToken.image))
            {
                if ("<EOF>".equals(parseException.tokenImage[0]))
                {
                    message = "Missing expression";
                }
            }

            if ("from".equalsIgnoreCase(parseException.currentToken.image))
            {
                if ("<EOF>".equals(parseException.tokenImage[0]))
                {
                    message = "Missing domain name";
                }
            }

            // handle invalid date part
            final String errorMessage = parseException.getMessage().replaceAll("\\s+", " ").trim();
            try
            {
                if (errorMessage.contains(INVALID_DATEPART_MARKER))
                {
                    final Matcher matcher = Pattern.compile("(?<=[^\\s][\"|>]\\s\")(.*?)(?=\"\")").matcher(errorMessage);
                    if (matcher.find())
                    {
                        final String datepart = matcher.group(0).trim();
                        message = "Datepart '" + datepart + "' not supported";
                    }
                }
            }
            catch (Exception e)
            {
                message = e.getMessage();
            }

            if (message == null)
            {
                message = errorMessage;
            }
        }
    }

    protected QueryParsingException(String message, Throwable throwable)
    {
        super(message, throwable);
    }

    public static QueryParsingException of(String message, Object... args)
    {
        return new QueryParsingException(String.format(message, args));
    }

    @Override
    public String getMessage()
    {
        return message != null ? message : super.getMessage();
    }
}
