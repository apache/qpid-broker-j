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


package org.apache.qpid.server.protocol.v1_0.type.transport;


import org.apache.qpid.server.protocol.v1_0.constants.Symbols;
import org.apache.qpid.server.protocol.v1_0.type.ErrorCondition;
import org.apache.qpid.server.protocol.v1_0.type.RestrictedType;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;

public class AmqpError implements ErrorCondition, RestrictedType<Symbol>
{
    public static final AmqpError INTERNAL_ERROR = new AmqpError(Symbols.AMQP_ERR_INTERNAL);
    public static final AmqpError NOT_FOUND = new AmqpError(Symbols.AMQP_ERR_NOT_FOUND);
    public static final AmqpError UNAUTHORIZED_ACCESS = new AmqpError(Symbols.AMQP_ERR_NOT_AUTHORIZED);
    public static final AmqpError DECODE_ERROR = new AmqpError(Symbols.AMQP_ERR_DECODE);
    public static final AmqpError RESOURCE_LIMIT_EXCEEDED = new AmqpError(Symbols.AMQP_ERR_RESOURCE_LIMIT_EXCEEDED);
    public static final AmqpError NOT_ALLOWED = new AmqpError(Symbols.AMQP_ERR_NOT_ALLOWED);
    public static final AmqpError INVALID_FIELD = new AmqpError(Symbols.AMQP_ERR_INVALID_FIELD);
    public static final AmqpError NOT_IMPLEMENTED = new AmqpError(Symbols.AMQP_ERR_NOT_IMPLEMENTED);
    public static final AmqpError RESOURCE_LOCKED = new AmqpError(Symbols.AMQP_ERR_RESOURCE_LOCKED);
    public static final AmqpError PRECONDITION_FAILED = new AmqpError(Symbols.AMQP_ERR_PRECONDITION_FAILED);
    public static final AmqpError RESOURCE_DELETED = new AmqpError(Symbols.AMQP_ERR_RESOURCE_DELETED);
    public static final AmqpError ILLEGAL_STATE = new AmqpError(Symbols.AMQP_ERR_ILLEGAL_STATE);
    public static final AmqpError FRAME_SIZE_TOO_SMALL = new AmqpError(Symbols.AMQP_ERR_FRAME_SIZE_TOO_SMALL);

    private final Symbol _val;


    private AmqpError(Symbol val)
    {
        _val = val;
    }

    public static AmqpError valueOf(Object obj)
    {
        if (obj instanceof Symbol)
        {
            Symbol val = (Symbol) obj;

            if (INTERNAL_ERROR._val.equals(val))
            {
                return INTERNAL_ERROR;
            }

            if (NOT_FOUND._val.equals(val))
            {
                return NOT_FOUND;
            }

            if (UNAUTHORIZED_ACCESS._val.equals(val))
            {
                return UNAUTHORIZED_ACCESS;
            }

            if (DECODE_ERROR._val.equals(val))
            {
                return DECODE_ERROR;
            }

            if (RESOURCE_LIMIT_EXCEEDED._val.equals(val))
            {
                return RESOURCE_LIMIT_EXCEEDED;
            }

            if (NOT_ALLOWED._val.equals(val))
            {
                return NOT_ALLOWED;
            }

            if (INVALID_FIELD._val.equals(val))
            {
                return INVALID_FIELD;
            }

            if (NOT_IMPLEMENTED._val.equals(val))
            {
                return NOT_IMPLEMENTED;
            }

            if (RESOURCE_LOCKED._val.equals(val))
            {
                return RESOURCE_LOCKED;
            }

            if (PRECONDITION_FAILED._val.equals(val))
            {
                return PRECONDITION_FAILED;
            }

            if (RESOURCE_DELETED._val.equals(val))
            {
                return RESOURCE_DELETED;
            }

            if (ILLEGAL_STATE._val.equals(val))
            {
                return ILLEGAL_STATE;
            }

            if (FRAME_SIZE_TOO_SMALL._val.equals(val))
            {
                return FRAME_SIZE_TOO_SMALL;
            }
        }

        final String message = String.format("Cannot convert '%s' into 'amqp-error'", obj);
        throw new IllegalArgumentException(message);
    }

    @Override
    public Symbol getValue()
    {
        return _val;
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

        final AmqpError amqpError = (AmqpError) o;

        return _val.equals(amqpError._val);
    }

    @Override
    public int hashCode()
    {
        return _val.hashCode();
    }

    @Override
    public String toString()
    {

        if (this == INTERNAL_ERROR)
        {
            return "internal-error";
        }

        if (this == NOT_FOUND)
        {
            return "not-found";
        }

        if (this == UNAUTHORIZED_ACCESS)
        {
            return "unauthorized-access";
        }

        if (this == DECODE_ERROR)
        {
            return "decode-error";
        }

        if (this == RESOURCE_LIMIT_EXCEEDED)
        {
            return "resource-limit-exceeded";
        }

        if (this == NOT_ALLOWED)
        {
            return "not-allowed";
        }

        if (this == INVALID_FIELD)
        {
            return "invalid-field";
        }

        if (this == NOT_IMPLEMENTED)
        {
            return "not-implemented";
        }

        if (this == RESOURCE_LOCKED)
        {
            return "resource-locked";
        }

        if (this == PRECONDITION_FAILED)
        {
            return "precondition-failed";
        }

        if (this == RESOURCE_DELETED)
        {
            return "resource-deleted";
        }

        if (this == ILLEGAL_STATE)
        {
            return "illegal-state";
        }

        if (this == FRAME_SIZE_TOO_SMALL)
        {
            return "frame-size-too-small";
        }

        else
        {
            return String.valueOf(_val);
        }
    }
}
