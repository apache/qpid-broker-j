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

/** UncaughtExceptionHandler implementation logs an exception and gracefully stops the broker */
public class GracefulShutdownExceptionHandler extends AbstractExceptionHandler implements Thread.UncaughtExceptionHandler
{
    @Override
    public void uncaughtException(final Thread thread, final Throwable throwable)
    {
        final boolean continueOnError = Boolean.getBoolean(CONTINUE_ON_ERROR_PROPERTY_NAME);
        try
        {
            printDiagnostic(thread, throwable, continueOnError);
            logError(throwable, continueOnError);
        }
        finally
        {
            if (!continueOnError)
            {
                ExitHandler.exit(1);
            }
        }
    }
}
