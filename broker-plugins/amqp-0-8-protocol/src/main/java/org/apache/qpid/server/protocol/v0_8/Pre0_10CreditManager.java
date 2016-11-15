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
package org.apache.qpid.server.protocol.v0_8;


import org.apache.qpid.server.flow.FlowCreditManager;
import org.apache.qpid.server.transport.ProtocolEngine;

public class Pre0_10CreditManager implements FlowCreditManager
{

    private final ProtocolEngine _protocolEngine;
    private volatile long _bytesCreditLimit;
    private volatile long _messageCreditLimit;

    private volatile long _bytesCredit;
    private volatile long _messageCredit;

    public Pre0_10CreditManager(long bytesCreditLimit,
                                long messageCreditLimit,
                                ProtocolEngine protocolEngine)
    {
        _protocolEngine = protocolEngine;
        _bytesCreditLimit = bytesCreditLimit;
        _messageCreditLimit = messageCreditLimit;
        _bytesCredit = bytesCreditLimit;
        _messageCredit = messageCreditLimit;
    }


    public synchronized void setCreditLimits(final long bytesCreditLimit, final long messageCreditLimit)
    {
        long bytesCreditChange = bytesCreditLimit - _bytesCreditLimit;
        long messageCreditChange = messageCreditLimit - _messageCreditLimit;

        if (bytesCreditChange != 0L)
        {
            _bytesCredit += bytesCreditChange;
        }

        if (messageCreditChange != 0L)
        {
            _messageCredit += messageCreditChange;
        }

        _bytesCreditLimit = bytesCreditLimit;
        _messageCreditLimit = messageCreditLimit;
    }

    public synchronized void restoreCredit(final long messageCredit, final long bytesCredit)
    {
        _messageCredit += messageCredit;
        if (_messageCredit > _messageCreditLimit)
        {
            throw new IllegalStateException(String.format(
                    "Consumer credit accounting error. Restored more credit than we ever had: messageCredit=%d  messageCreditLimit=%d",
                    _messageCredit,
                    _messageCreditLimit));
        }

        _bytesCredit += bytesCredit;
        if (_bytesCredit > _bytesCreditLimit)
        {
            throw new IllegalStateException(String.format(
                    "Consumer credit accounting error. Restored more credit than we ever had: bytesCredit=%d  bytesCreditLimit=%d",
                    _bytesCredit,
                    _bytesCreditLimit));
        }
    }

    public synchronized boolean hasCredit()
    {
        return (_bytesCreditLimit == 0L || _bytesCredit > 0)
               && (_messageCreditLimit == 0L || _messageCredit > 0)
               && !_protocolEngine.isTransportBlockedForWriting();
    }

    public synchronized boolean useCreditForMessage(final long msgSize)
    {
        if (_protocolEngine.isTransportBlockedForWriting())
        {
            return false;
        }

        if (_messageCreditLimit != 0)
        {
            if (_messageCredit <= 0)
            {
                return false;
            }
        }
        if (_bytesCreditLimit != 0)
        {
            if ((_bytesCredit < msgSize) && (_bytesCredit != _bytesCreditLimit))
            {
                return false;
            }
        }

        _messageCredit--;
        _bytesCredit -= msgSize;
        return true;
    }
}
