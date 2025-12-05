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

package org.apache.qpid.server.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractExceptionHandler
{
    protected static final String CONTINUE_ON_ERROR_PROPERTY_NAME = "qpid.broker.exceptionHandler.continue";

    protected String getMessage(final boolean continueOnError)
    {
        return continueOnError
                ? "# Forced to continue by JVM setting 'qpid.broker.exceptionHandler.continue'"
                : "# Exiting";
    }

    protected void printDiagnostic(final Thread thread, final Throwable throwable, final boolean continueOnError)
    {
        System.err.println("########################################################################");
        System.err.println("#");
        System.err.print("# Unhandled Exception ");
        System.err.print(throwable.toString());
        System.err.print(" in Thread ");
        System.err.println(thread.getName());
        System.err.println("#");
        System.err.println(getMessage(continueOnError));
        System.err.println("#");
        System.err.println("########################################################################");
        throwable.printStackTrace(System.err);
    }

    protected void logError(final Throwable throwable, final boolean continueOnError)
    {
        final Logger logger = LoggerFactory.getLogger("org.apache.qpid.server.Main");
        final String message = "Uncaught exception, " + (continueOnError ? "continuing." : "shutting down.");
        logger.error(message, throwable);
    }
}
