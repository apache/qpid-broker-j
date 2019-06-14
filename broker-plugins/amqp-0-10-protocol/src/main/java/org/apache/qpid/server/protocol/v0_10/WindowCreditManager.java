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
package org.apache.qpid.server.protocol.v0_10;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowCreditManager implements FlowCreditManager_0_10
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WindowCreditManager.class);

    private volatile long _bytesCreditLimit;
    private volatile long _messageCreditLimit;

    private volatile long _bytesUsed;
    private volatile long _messageUsed;

    public WindowCreditManager(long bytesCreditLimit,
                               long messageCreditLimit)
    {
        _bytesCreditLimit = bytesCreditLimit;
        _messageCreditLimit = messageCreditLimit;
    }

    public synchronized long getMessageCreditLimit()
    {
        return _messageCreditLimit;
    }

    synchronized long getMessageCredit()
    {
         return _messageCreditLimit == -1L
                    ? Long.MAX_VALUE
                    : _messageUsed < _messageCreditLimit ? _messageCreditLimit - _messageUsed : 0L;
    }

    synchronized long getBytesCredit()
    {
        return _bytesCreditLimit == -1L
                    ? Long.MAX_VALUE
                    : _bytesUsed < _bytesCreditLimit ? _bytesCreditLimit - _bytesUsed : 0L;
    }

    @Override
    public synchronized void restoreCredit(final long messageCredit, final long bytesCredit)
    {
        if (_messageCreditLimit >= 0L)
        {
            _messageUsed -= messageCredit;
            if (_messageUsed < 0L)
            {
                LOGGER.warn("Message credit used value was negative: " + _messageUsed);
                _messageUsed = 0;
            }
        }

        if (_bytesCreditLimit >= 0L)
        {
            _bytesUsed -= bytesCredit;
            if (_bytesUsed < 0L)
            {
                LOGGER.warn("Bytes credit used value was negative: " + _bytesUsed);
                _bytesUsed = 0;
            }
        }
    }

    @Override
    public synchronized boolean hasCredit()
    {
        return (_bytesCreditLimit < 0L || _bytesCreditLimit > _bytesUsed)
                && (_messageCreditLimit < 0L || _messageCreditLimit > _messageUsed);
    }

    @Override
    public synchronized boolean useCreditForMessage(final long msgSize)
    {
        if(_messageCreditLimit >= 0L)
        {
            if(_messageUsed < _messageCreditLimit)
            {
                if(_bytesCreditLimit < 0L)
                {
                    _messageUsed++;

                    return true;
                }
                else if(_bytesUsed + msgSize <= _bytesCreditLimit)
                {
                    _messageUsed++;
                    _bytesUsed += msgSize;

                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }
        else if(_bytesCreditLimit >= 0L)
        {
            if(_bytesUsed + msgSize <= _bytesCreditLimit)
            {
                 _bytesUsed += msgSize;

                return true;
            }
            else
            {
                return false;
            }

        }
        else
        {
            return true;
        }

    }


    @Override
    public synchronized void addCredit(long count, long bytes)
    {
        if(bytes == INFINITE_CREDIT)
        {
            _bytesCreditLimit = -1L;
        }
        else if(_bytesCreditLimit >= 0L)
        {
            _bytesCreditLimit += bytes;
            if (_bytesCreditLimit < 0L)
            {
                LOGGER.warn("Bytes credit wraparound: attempt to add {} bytes credit to existing total of {}",
                         bytes,
                         _bytesCreditLimit - bytes);
                _bytesCreditLimit = Long.MAX_VALUE;
            }
        }

        if(count == INFINITE_CREDIT)
        {
            _messageCreditLimit = -1L;
        }
        else if(_messageCreditLimit >= 0L)
        {
            _messageCreditLimit += count;
            if (_messageCreditLimit < 0L)
            {
                LOGGER.warn("Message credit wraparound: attempt to add {} message credit to existing total of {}",
                         count,
                         _messageCreditLimit - count);
                _messageCreditLimit = Long.MAX_VALUE;
            }
        }
    }

    @Override
    public synchronized void clearCredit()
    {
        _bytesCreditLimit = 0L;
        _messageCreditLimit = 0L;
    }
}
