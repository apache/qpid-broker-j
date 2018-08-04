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

package org.apache.qpid.server.protocol.v1_0;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;

public class SequenceNumber implements Comparable<SequenceNumber>
{
    private volatile int _seqNo;

    public SequenceNumber(int seqNo)
    {
        _seqNo = seqNo;
    }

    public SequenceNumber incr()
    {
        _seqNo++;
        return this;
    }

    public SequenceNumber decr()
    {
        _seqNo--;
        return this;
    }

    public SequenceNumber add(int i)
    {
        _seqNo+=i;
        return this;
    }
    public SequenceNumber subtract(final int value)
    {
        _seqNo-=value;
        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        SequenceNumber that = (SequenceNumber) o;

        return _seqNo == that._seqNo;
    }

    @Override
    public int hashCode()
    {
        return _seqNo;
    }

    @Override
    public int compareTo(SequenceNumber o)
    {
        return _seqNo - o._seqNo;
    }

    @Override
    public String toString()
    {
        return "SN{" + _seqNo + '}';
    }

    public int intValue()
    {
        return _seqNo;
    }

    public long longValue()
    {
        return  ((long) _seqNo) & 0xFFFFFFFFL;
    }

    public UnsignedInteger unsignedIntegerValue()
    {
        return UnsignedInteger.valueOf(_seqNo);
    }
}
