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

package org.apache.qpid.tests.protocol.v0_8;

import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.tests.protocol.Response;

public class PerformativeResponse implements Response<AMQBody>
{
    private final int _channel;
    private final long _size;
    private final AMQBody _body;

    public PerformativeResponse(int channel, long size, final AMQBody body)
    {
        _channel = channel;
        _size = size;
        _body = body;
    }

    @Override
    public AMQBody getBody()
    {
        return _body;
    }

    @Override
    public String toString()
    {
        return "PerformativeResponse{" +
               "_channel=" + _channel +
               ", _size=" + _size +
               ", _body=" + _body +
               '}';
    }
}
