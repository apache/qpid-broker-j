/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.qpid.server.security.access.config;

import java.util.Locale;

/**
 * An enumeration of all possible actions that can form part of an access control v2 rule.
 */
public enum LegacyOperation
{
    ALL,
    CONSUME,
    PUBLISH,
    CREATE,
    ACCESS,
    BIND,
    UNBIND,
    DELETE,
    PURGE,
    UPDATE,
    CONFIGURE,
    ACCESS_LOGS,
    SHUTDOWN,
    INVOKE;

    private final String _description;

    LegacyOperation()
    {
        final String name = name();
        _description = name.substring(0, 1).toUpperCase(Locale.ENGLISH) + name.substring(1).toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String toString()
    {
        return _description;
    }
}
