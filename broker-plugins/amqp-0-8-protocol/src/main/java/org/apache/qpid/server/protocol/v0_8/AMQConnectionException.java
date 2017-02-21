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

import org.apache.qpid.server.AMQException;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;

/**
 * AMQConnectionException indicates that an error that requires the channel to be closed has occurred.
 */
public class AMQConnectionException extends AMQException
{
    private final int _classId;
    private final int _methodId;

    private final MethodRegistry _methodRegistry;

    public AMQConnectionException(int errorCode, String msg, int classId, int methodId, MethodRegistry methodRegistry,
                                  Throwable cause)
    {
        super(errorCode, msg, cause);
        _classId = classId;
        _methodId = methodId;
        _methodRegistry = methodRegistry;

    }


    public AMQFrame getCloseFrame()
    {
        return new AMQFrame(0,
                            _methodRegistry.createConnectionCloseBody(getErrorCode(),
                                                                      AMQShortString.validValueOf(getMessage()),
                                                                      _classId,
                                                                      _methodId));

    }

    @Override
    public AMQException cloneForCurrentThread()
    {
        return new AMQConnectionException(getErrorCode(), getMessage(), _classId, _methodId, _methodRegistry, getCause());
    }
}
