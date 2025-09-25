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

import org.apache.qpid.server.protocol.v1_0.constants.SymbolTexts;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.CompositeType;
import org.apache.qpid.server.protocol.v1_0.CompositeTypeField;

@CompositeType( symbolicDescriptor = SymbolTexts.AMQP_TXN_DISCHARGE, numericDescriptor = 0x0000000000000032L)
public class Discharge
{
    @CompositeTypeField(index = 0, mandatory = true)
    private Binary _txnId;

    @CompositeTypeField(index = 1)
    private Boolean _fail;

    public Binary getTxnId()
    {
        return _txnId;
    }

    public void setTxnId(Binary txnId)
    {
        _txnId = txnId;
    }

    public Boolean getFail()
    {
        return _fail;
    }

    public void setFail(Boolean fail)
    {
        _fail = fail;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Discharge{");
        final int origLength = builder.length();

        if (_txnId != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("txnId=").append(_txnId);
        }

        if (_fail != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("fail=").append(_fail);
        }

        builder.append('}');
        return builder.toString();
    }
}
