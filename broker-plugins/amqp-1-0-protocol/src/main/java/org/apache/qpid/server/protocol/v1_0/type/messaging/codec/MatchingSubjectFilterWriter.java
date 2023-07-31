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

package org.apache.qpid.server.protocol.v1_0.type.messaging.codec;

import org.apache.qpid.server.protocol.v1_0.codec.AbstractDescribedTypeWriter;
import org.apache.qpid.server.protocol.v1_0.codec.UnsignedLongWriter;
import org.apache.qpid.server.protocol.v1_0.codec.ValueWriter;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MatchingSubjectFilter;

public class MatchingSubjectFilterWriter extends AbstractDescribedTypeWriter<MatchingSubjectFilter>
{
    private static final ValueWriter<UnsignedLong> DESCRIPTOR_WRITER = UnsignedLongWriter.getWriter(0x0000468C00000001L);
    private static final Factory<MatchingSubjectFilter> FACTORY = MatchingSubjectFilterWriter::new;

    public MatchingSubjectFilterWriter(final Registry registry,
                                       final MatchingSubjectFilter object)
    {
        super(DESCRIPTOR_WRITER, registry.getValueWriter(object.getValue()));
    }

    public static void register(Registry registry)
    {
        registry.register(MatchingSubjectFilter.class, FACTORY);
    }
}
