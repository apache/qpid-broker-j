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
package org.apache.qpid.server.security.access;

import java.util.Objects;

public final class Operation
{
    public static final Operation CREATE = new Operation(OperationType.CREATE);
    public static final Operation UPDATE = new Operation(OperationType.UPDATE);
    public static final Operation DELETE = new Operation(OperationType.DELETE);
    public static final Operation DISCOVER = new Operation(OperationType.DISCOVER);
    public static final Operation READ = new Operation(OperationType.READ);

    private final OperationType _type;
    private final String _name;


    private Operation(final OperationType type)
    {
        this(type, type.name());
    }

    private Operation(final OperationType type, String name)
    {
        _type = type;
        _name = name;
    }

    public OperationType getType()
    {
        return _type;
    }

    public String getName()
    {
        return _name;
    }


    public static Operation CREATE()
    {
        return CREATE;
    }

    public static Operation UPDATE()
    {
        return UPDATE;
    }

    public static Operation DELETE()
    {
        return DELETE;
    }

    public static Operation DISCOVER()
    {
        return DISCOVER;
    }

    public static Operation READ()
    {
        return READ;
    }

    public static Operation INVOKE_METHOD(String name)
    {
        return new Operation(OperationType.INVOKE_METHOD, name);
    }


    public static Operation PERFORM_ACTION(String name)
    {
        return new Operation(OperationType.PERFORM_ACTION, name);
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        final Operation operation = (Operation) o;
        return getType() == operation.getType() &&
               Objects.equals(getName(), operation.getName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getType(), getName());
    }

    @Override
    public String toString()
    {
        return "Operation[" +_type + (_name.equals(_type.name()) ? "" : ("("+_name+")")) + "]";
    }
}
