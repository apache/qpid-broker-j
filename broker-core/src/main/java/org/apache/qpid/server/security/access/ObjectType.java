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
package org.apache.qpid.server.security.access;

import static org.apache.qpid.server.security.access.Operation.ACCESS;
import static org.apache.qpid.server.security.access.Operation.ACCESS_LOGS;
import static org.apache.qpid.server.security.access.Operation.BIND;
import static org.apache.qpid.server.security.access.Operation.CONFIGURE;
import static org.apache.qpid.server.security.access.Operation.CONSUME;
import static org.apache.qpid.server.security.access.Operation.CREATE;
import static org.apache.qpid.server.security.access.Operation.DELETE;
import static org.apache.qpid.server.security.access.Operation.PUBLISH;
import static org.apache.qpid.server.security.access.Operation.PURGE;
import static org.apache.qpid.server.security.access.Operation.SHUTDOWN;
import static org.apache.qpid.server.security.access.Operation.UNBIND;
import static org.apache.qpid.server.security.access.Operation.UPDATE;

import java.util.EnumSet;
import java.util.Set;

/**
 * An enumeration of all possible object types that can form part of an access control v2 rule.
 * 
 * Each object type is valid only for a certain set of {@link Operation}s, which are passed as a list to
 * the constructor, and can be checked using the {@link #isSupported(Operation)} method.
 */
public enum ObjectType
{
    ALL(EnumSet.allOf(Operation.class)),
    VIRTUALHOSTNODE(Operation.ALL, CREATE, DELETE, UPDATE),
    VIRTUALHOST(Operation.ALL, ACCESS, CREATE, DELETE, UPDATE, ACCESS_LOGS),
    MANAGEMENT(Operation.ALL, ACCESS),
    QUEUE(Operation.ALL, CREATE, DELETE, PURGE, CONSUME, UPDATE),
    EXCHANGE(Operation.ALL, ACCESS, CREATE, DELETE, BIND, UNBIND, PUBLISH, UPDATE),
    METHOD(Operation.ALL, ACCESS, UPDATE),
    USER(Operation.ALL, CREATE, DELETE, UPDATE),
    GROUP(Operation.ALL, CREATE, DELETE, UPDATE),
    BROKER(Operation.ALL, CONFIGURE, ACCESS_LOGS, SHUTDOWN);

    private EnumSet<Operation> _operations;


    ObjectType(Operation first, Operation... rest)
    {
        this(EnumSet.of(first, rest));
    }

    ObjectType(EnumSet<Operation> operations)
    {
        _operations = operations;
    }
    
    public Set<Operation> getOperations()
    {
        return _operations;
    }
    
    public boolean isSupported(Operation operation)
    {
        return _operations.contains(operation);
    }
    
    public String toString()
    {
        String name = name();
        return name.charAt(0) + name.substring(1).toLowerCase();
    }

}
