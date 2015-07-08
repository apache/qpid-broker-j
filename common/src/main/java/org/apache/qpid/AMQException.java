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
package org.apache.qpid;

import org.apache.qpid.protocol.AMQConstant;


public class AMQException extends QpidException
{
    private final AMQConstant _errorCode;

    private final boolean _isHardError;

    public AMQException(final AMQConstant errorCode, final String message)
    {
        this(errorCode, message, null);
    }

    /**
     * Constructor for a Protocol Exception 
     *
     * @param msg       A description of the reason of this exception .
     * @param errorCode A string specifying the error code of this exception.
     * @param cause     The linked Execption.
     */
    public AMQException(AMQConstant errorCode, String msg, Throwable cause)
    {
        this(errorCode, true, msg, cause);
    }

    public AMQException(final AMQConstant errorCode,
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
        return getClass().getName() + ": " + getMessage() + (_errorCode == null ? "" : " [error code " + _errorCode + "]");
    }

    /**
     * Gets the AMQ protocol exception code associated with this exception.
     *
     * @return The AMQ protocol exception code associated with this exception.
     */
    public AMQConstant getErrorCode()
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
        Class<?>[] paramClasses = {AMQConstant.class, String.class, Throwable.class};
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

}
