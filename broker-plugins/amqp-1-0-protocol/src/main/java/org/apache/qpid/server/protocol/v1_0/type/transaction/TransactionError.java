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
package org.apache.qpid.server.protocol.v1_0.type.transaction;


import org.apache.qpid.server.protocol.v1_0.type.ErrorCondition;
import org.apache.qpid.server.protocol.v1_0.type.RestrictedType;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;

public class TransactionError implements ErrorCondition, RestrictedType<Symbol>
{
    private final Symbol _val;

    public static final TransactionError UNKNOWN_ID =
            new TransactionError(Symbol.valueOf("amqp:transaction:unknown-id"));

    public static final TransactionError TRANSACTION_ROLLBACK =
            new TransactionError(Symbol.valueOf("amqp:transaction:rollback"));

    public static final TransactionError TRANSACTION_TIMEOUT =
            new TransactionError(Symbol.valueOf("amqp:transaction:timeout"));

    private TransactionError(Symbol val)
    {
        _val = val;
    }

    @Override
    public Symbol getValue()
    {
        return _val;
    }

    @Override
    public String toString()
    {
        if (this == UNKNOWN_ID)
        {
            return "unknown-id";
        }

        if (this == TRANSACTION_ROLLBACK)
        {
            return "transaction-rollback";
        }

        if (this == TRANSACTION_TIMEOUT)
        {
            return "transaction-timeout";
        }

        else
        {
            return String.valueOf(_val);
        }
    }

    public static TransactionError valueOf(Object obj)
    {
        if (obj instanceof Symbol)
        {
            Symbol val = (Symbol) obj;

            if (UNKNOWN_ID._val.equals(val))
            {
                return UNKNOWN_ID;
            }

            if (TRANSACTION_ROLLBACK._val.equals(val))
            {
                return TRANSACTION_ROLLBACK;
            }

            if (TRANSACTION_TIMEOUT._val.equals(val))
            {
                return TRANSACTION_TIMEOUT;
            }
        }

        final String message = String.format("Cannot convert '%s' into 'transaction-error'", obj);
        throw new IllegalArgumentException(message);
    }
}
