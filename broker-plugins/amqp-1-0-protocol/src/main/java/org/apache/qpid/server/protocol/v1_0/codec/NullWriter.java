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

package org.apache.qpid.server.protocol.v1_0.codec;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

public class NullWriter implements ValueWriter<Void>
{
    private static final NullWriter INSTANCE = new NullWriter();

    @Override
    public int getEncodedSize()
    {
        return 1;
    }

    @Override
    public void writeToBuffer(QpidByteBuffer buffer)
    {

        buffer.put((byte)0x40);
    }

    private static final Factory<Void> FACTORY = new Factory<Void>()
                                            {

                                                @Override
                                                public ValueWriter<Void> newInstance(final Registry registry,
                                                                                     final Void object)
                                                {
                                                    return INSTANCE;
                                                }
                                            };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(Void.TYPE, FACTORY);
    }
}
