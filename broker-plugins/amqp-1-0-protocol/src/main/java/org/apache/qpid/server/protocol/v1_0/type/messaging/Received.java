
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


package org.apache.qpid.server.protocol.v1_0.type.messaging;


import org.apache.qpid.server.protocol.v1_0.type.CompositeTypeField;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;

public class Received implements DeliveryState
{
    @CompositeTypeField(mandatory = true)
    private UnsignedInteger _sectionNumber;

    @CompositeTypeField(mandatory = true)
    private UnsignedLong _sectionOffset;

    public UnsignedInteger getSectionNumber()
    {
        return _sectionNumber;
    }

    public void setSectionNumber(UnsignedInteger sectionNumber)
    {
        _sectionNumber = sectionNumber;
    }

    public UnsignedLong getSectionOffset()
    {
        return _sectionOffset;
    }

    public void setSectionOffset(UnsignedLong sectionOffset)
    {
        _sectionOffset = sectionOffset;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Received{");
        final int origLength = builder.length();

        if (_sectionNumber != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("sectionNumber=").append(_sectionNumber);
        }

        if (_sectionOffset != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("sectionOffset=").append(_sectionOffset);
        }

        builder.append('}');
        return builder.toString();
    }
}
