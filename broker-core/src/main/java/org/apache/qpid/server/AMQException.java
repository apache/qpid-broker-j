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
package org.apache.qpid.server;

import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.QpidException;


public class AMQException extends QpidException
{
    private final int _errorCode;

    private final boolean _isHardError;

    public AMQException(final int errorCode, final String message)
    {
        this(errorCode, message, null);
    }


    public AMQException(int errorCode, String msg, Throwable cause)
    {
        this(errorCode, true, msg, cause);
    }


    public AMQException(final int errorCode,
                        final boolean isHardError,
                        final String message,
                        final Throwable cause)
    {
        super(message, cause);
        _errorCode = errorCode;
        _isHardError = isHardError;
    }



    @Override
    public String toString()
    {
        return getClass().getName() + ": " + getMessage() + (_errorCode == 0 ? "" : " [error code: " + _errorCode + "("+getDefaultDescription(_errorCode) + ")]");
    }

    public int getErrorCode()
    {
        return _errorCode;
    }

    public boolean isHardError()
    {
        return _isHardError;
    }

    @Override
    public AMQException cloneForCurrentThread()
    {
        Class amqeClass = this.getClass();
        Class<?>[] paramClasses = {Integer.TYPE, String.class, Throwable.class};
        Object[] params = {_errorCode, getMessage(), this};

        AMQException newAMQE;

        try
        {
            newAMQE = (AMQException) amqeClass.getConstructor(paramClasses).newInstance(params);
        }
        catch (Exception creationException)
        {
            newAMQE = new AMQException(_errorCode, _isHardError, getMessage(), this);
        }

        return newAMQE;
    }


    private static String getDefaultDescription(int errorCode)
    {
        switch(errorCode)
        {
            case ErrorCodes.REPLY_SUCCESS:
                return "reply success";
            case ErrorCodes.NOT_DELIVERED:
                return "not delivered";
            case ErrorCodes.MESSAGE_TOO_LARGE:
                return "message too large";
            case ErrorCodes.NO_ROUTE:
                return "no route";
            case ErrorCodes.NO_CONSUMERS:
                return "no consumers";
            case ErrorCodes.CONNECTION_FORCED:
                return "connection forced";
            case ErrorCodes.INVALID_PATH:
                return "invalid path";
            case ErrorCodes.ACCESS_REFUSED:
                return "access refused";
            case ErrorCodes.NOT_FOUND:
                return "not found";
            case ErrorCodes.ALREADY_EXISTS:
                return "Already exists";
            case ErrorCodes.IN_USE:
                return "In use";
            case ErrorCodes.INVALID_ROUTING_KEY:
                return "routing key invalid";
            case ErrorCodes.REQUEST_TIMEOUT:
                return "Request Timeout";
            case ErrorCodes.ARGUMENT_INVALID:
                return "argument invalid";
            case ErrorCodes.FRAME_ERROR:
                return "frame error";
            case ErrorCodes.SYNTAX_ERROR:
                return "syntax error";
            case ErrorCodes.COMMAND_INVALID:
                return "command invalid";
            case ErrorCodes.CHANNEL_ERROR:
                return "channel error";
            case ErrorCodes.RESOURCE_ERROR:
                return "resource error";
            case ErrorCodes.NOT_ALLOWED:
                return "not allowed";
            case ErrorCodes.NOT_IMPLEMENTED:
                return "not implemented";
            case ErrorCodes.INTERNAL_ERROR:
                return "internal error";
            case ErrorCodes.INVALID_ARGUMENT:
                return "invalid argument";
            case ErrorCodes.UNSUPPORTED_CLIENT_PROTOCOL_ERROR:
                return "client unsupported protocol";
            default:
                return "<<unknown>>";
        }
    }

}
