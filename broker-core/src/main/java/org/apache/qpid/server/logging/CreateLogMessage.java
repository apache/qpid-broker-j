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
package org.apache.qpid.server.logging;

import org.apache.qpid.server.model.ConfiguredObject;

public class CreateLogMessage implements LogMessage
{
    private final String _hierarchy;
    private final String _logMessage;

    public CreateLogMessage(final Outcome outcome,
                            final Class<? extends ConfiguredObject> categoryClass,
                            final String name,
                            final String attributes)
    {
        _hierarchy = AbstractMessageLogger.DEFAULT_LOG_HIERARCHY_PREFIX
                     + "." + categoryClass.getSimpleName().toLowerCase()
                     + ".create";
        _logMessage = String.format("%s (%s) : Create : %s : %s",
                                    categoryClass.getSimpleName(),
                                    name,
                                    outcome,
                                    attributes);
    }

    @Override
    public String getLogHierarchy()
    {
        return _hierarchy;
    }

    @Override
    public String toString()
    {
        return _logMessage;
    }
}
