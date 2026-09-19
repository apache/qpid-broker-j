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
import org.apache.qpid.server.protocol.ErrorCodes;

import java.io.Serial;

/**
 * AMQFrameDecodingException indicates that an AMQP frame cannot be decoded because it does not have the correct
 * format as defined by the protocol.
 */
public class AMQFrameDecodingException extends AMQException
{
    @Serial
    private static final long serialVersionUID = 1L;

    private final int _classId;
    private final int _methodId;

    public AMQFrameDecodingException(final String message, final Throwable cause)
    {
        this(ErrorCodes.FRAME_ERROR, message, 0, 0, cause);
    }

    public AMQFrameDecodingException(final String message)
    {
        this(ErrorCodes.FRAME_ERROR, message, 0, 0, null);
    }


    public AMQFrameDecodingException(final int errorCode, final String message, final Throwable cause)
    {
        this(errorCode, message, 0, 0, cause);
    }

    AMQFrameDecodingException(final int errorCode,
                              final String message,
                              final int classId,
                              final int methodId,
                              final Throwable cause)
    {
        super(errorCode, message, cause);
        _classId = classId;
        _methodId = methodId;
    }

    public static AMQFrameDecodingException forDecodingFailure(final String context,
                                                               final RuntimeException cause)
    {
        return forDecodingFailure(context, 0, 0, cause);
    }

    static AMQFrameDecodingException forDecodingFailure(final String context,
                                                        final int classId,
                                                        final int methodId,
                                                        final RuntimeException cause)
    {
        final int errorCode = cause instanceof AMQValueNestingException
                ? ErrorCodes.RESOURCE_ERROR
                : ErrorCodes.FRAME_ERROR;
        final String detail = cause.getMessage();
        final String message = detail == null || detail.isEmpty() ? context : context + ": " + detail;
        return new AMQFrameDecodingException(errorCode, message, classId, methodId, cause);
    }

    int getClassId()
    {
        return _classId;
    }

    int getMethodId()
    {
        return _methodId;
    }
}
