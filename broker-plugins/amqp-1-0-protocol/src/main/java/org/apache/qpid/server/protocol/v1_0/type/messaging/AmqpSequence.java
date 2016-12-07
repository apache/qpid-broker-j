
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


import java.util.Collections;
import java.util.List;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoder;
import org.apache.qpid.server.protocol.v1_0.type.Binary;

public class AmqpSequence implements NonEncodingRetainingSection<List>
{

    private final List _value;

    public AmqpSequence(List value)
    {
        _value = value;
    }

    public List getValue()
    {
        return _value;
    }


    @Override
    public String toString()
    {
        return "AmqpSequence{" + _value + '}';
    }

    @Override
    public AmqpSequenceSection createEncodingRetainingSection(final SectionEncoder encoder)
    {
        encoder.reset();
        encoder.encodeObject(this);
        Binary encodedOutput = encoder.getEncoding();
        final QpidByteBuffer buf = QpidByteBuffer.wrap(encodedOutput.asByteBuffer());
        return new AmqpSequenceSection(this, Collections.singletonList(buf), encoder.getRegistry());
    }
}
