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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

import static org.apache.qpid.server.security.access.config.LegacyOperation.ACCESS;
import static org.apache.qpid.server.security.access.config.LegacyOperation.ACCESS_LOGS;
import static org.apache.qpid.server.security.access.config.LegacyOperation.BIND;
import static org.apache.qpid.server.security.access.config.LegacyOperation.CONFIGURE;
import static org.apache.qpid.server.security.access.config.LegacyOperation.CONSUME;
import static org.apache.qpid.server.security.access.config.LegacyOperation.CREATE;
import static org.apache.qpid.server.security.access.config.LegacyOperation.DELETE;
import static org.apache.qpid.server.security.access.config.LegacyOperation.INVOKE;
import static org.apache.qpid.server.security.access.config.LegacyOperation.PUBLISH;
import static org.apache.qpid.server.security.access.config.LegacyOperation.PURGE;
import static org.apache.qpid.server.security.access.config.LegacyOperation.SHUTDOWN;
import static org.apache.qpid.server.security.access.config.LegacyOperation.UNBIND;
import static org.apache.qpid.server.security.access.config.LegacyOperation.UPDATE;

/**
 * An enumeration of all possible object types that can form part of an access control v2 rule.
 * <p>
 * Each object type is valid only for a certain set of {@link LegacyOperation}s, which are passed as a list to
 * the constructor, and can be checked using the {@link #isSupported(LegacyOperation)} method.
 */
public enum ObjectType
{
    ALL,
    VIRTUALHOSTNODE(LegacyOperation.ALL, CREATE, DELETE, UPDATE, INVOKE),
    VIRTUALHOST(LegacyOperation.ALL, ACCESS, CREATE, DELETE, UPDATE, ACCESS_LOGS, INVOKE),
    MANAGEMENT(LegacyOperation.ALL, ACCESS),
    QUEUE(LegacyOperation.ALL, CREATE, DELETE, PURGE, CONSUME, UPDATE, INVOKE),
    EXCHANGE(LegacyOperation.ALL, ACCESS, CREATE, DELETE, BIND, UNBIND, PUBLISH, UPDATE, INVOKE),
    METHOD(LegacyOperation.ALL, ACCESS, UPDATE),
    USER(LegacyOperation.ALL, CREATE, DELETE, UPDATE, INVOKE),
    GROUP(LegacyOperation.ALL, CREATE, DELETE, UPDATE, INVOKE),
    BROKER(LegacyOperation.ALL, CONFIGURE, ACCESS_LOGS, SHUTDOWN, INVOKE);

    private final EnumSet<LegacyOperation> _operations;

    private final String _description;

    ObjectType(LegacyOperation... rest)
    {
        _operations = EnumSet.of(LegacyOperation.ALL, rest);
        _description = description();
    }

    ObjectType()
    {
        _operations = EnumSet.allOf(LegacyOperation.class);
        _description = description();
    }

    private String description()
    {
        final String name = name();
        return name.substring(0, 1).toUpperCase(Locale.ENGLISH) + name.substring(1).toLowerCase(Locale.ENGLISH);
    }

    public Set<LegacyOperation> getOperations()
    {
        return Collections.unmodifiableSet(_operations);
    }

    public boolean isSupported(LegacyOperation operation)
    {
        return _operations.contains(operation);
    }

    @Override
    public String toString()
    {
        return _description;
    }
}
