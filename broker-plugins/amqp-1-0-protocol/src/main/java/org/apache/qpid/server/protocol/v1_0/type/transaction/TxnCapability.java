
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


import org.apache.qpid.server.protocol.v1_0.constants.Symbols;
import org.apache.qpid.server.protocol.v1_0.type.RestrictedType;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;

public class TxnCapability implements org.apache.qpid.server.protocol.v1_0.type.TxnCapability, RestrictedType<Symbol>
{
    private final Symbol _val;

    public static final TxnCapability LOCAL_TXN = new TxnCapability(Symbols.AMQP_LOCAL_TXN);
    public static final TxnCapability DISTRIBUTED_TXN = new TxnCapability(Symbols.AMQP_DISTRIBUTED_TXN);
    public static final TxnCapability PROMOTABLE_TXN = new TxnCapability(Symbols.AMQP_PROMOTABLE_TXN);
    public static final TxnCapability MULTI_TXNS_PER_SSN = new TxnCapability(Symbols.AMQP_MULTI_TXN_PER_SESSION);
    public static final TxnCapability MULTI_SSNS_PER_TXN = new TxnCapability(Symbols.AMQP_MULTI_SESSIONS_PER_TXN);

    private TxnCapability(Symbol val)
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

        if (this == LOCAL_TXN)
        {
            return "local-txn";
        }

        if (this == DISTRIBUTED_TXN)
        {
            return "distributed-txn";
        }

        if (this == PROMOTABLE_TXN)
        {
            return "promotable-txn";
        }

        if (this == MULTI_TXNS_PER_SSN)
        {
            return "multi-txns-per-ssn";
        }

        if (this == MULTI_SSNS_PER_TXN)
        {
            return "multi-ssns-per-txn";
        }

        else
        {
            return String.valueOf(_val);
        }
    }

    public static TxnCapability valueOf(Object obj)
    {
        if (obj instanceof Symbol)
        {
            Symbol val = (Symbol) obj;

            if (LOCAL_TXN._val.equals(val))
            {
                return LOCAL_TXN;
            }

            if (DISTRIBUTED_TXN._val.equals(val))
            {
                return DISTRIBUTED_TXN;
            }

            if (PROMOTABLE_TXN._val.equals(val))
            {
                return PROMOTABLE_TXN;
            }

            if (MULTI_TXNS_PER_SSN._val.equals(val))
            {
                return MULTI_TXNS_PER_SSN;
            }

            if (MULTI_SSNS_PER_TXN._val.equals(val))
            {
                return MULTI_SSNS_PER_TXN;
            }
        }

        final String message = String.format("Cannot convert '%s' into 'txn-capability'", obj);
        throw new IllegalArgumentException(message);
    }
}
