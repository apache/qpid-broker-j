/*
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
 */

package org.apache.qpid.server.protocol.v1_0.codec;

import java.util.Collections;
import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

class ZeroListConstructor implements TypeConstructor<List>
{
    private static final ZeroListConstructor INSTANCE = new ZeroListConstructor();

    ZeroListConstructor()
    {
    }

    @Override
    public List construct(final QpidByteBuffer in, final ValueHandler handler)
    {
        return Collections.EMPTY_LIST;
    }

    public static ZeroListConstructor getInstance()
    {
        return INSTANCE;
    }

}
