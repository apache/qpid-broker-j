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
package org.apache.qpid.server;

/**
 * AMQException forms the root exception of all exceptions relating to the AMQ protocol. It provides space to associate
 * a required AMQ error code with the exception, which is a numeric value, with a meaning defined by the protocol.
 */
public class QpidException extends Exception
{
    public QpidException(String msg)
    {
        this(msg, null);
    }

    public QpidException(String msg, Throwable cause)
    {
        super(msg == null ? "" : msg, cause);
    }

    @Override
    public String toString()
    {
        return getClass().getName() + ": " + getMessage();
    }

    /**
     * Rethrown this exception as a new exception.
     *
     * Attempt to create a new exception of the same class if they have the default constructor of:
     * {String, Throwable}.
     *
     * @return cloned exception
     */
    public QpidException cloneForCurrentThread()
    {
        Class amqeClass = this.getClass();
        Class<?>[] paramClasses = {String.class, Throwable.class};
        Object[] params = {getMessage(), this};

        QpidException newAMQE;

        try
        {
            newAMQE = (QpidException) amqeClass.getConstructor(paramClasses).newInstance(params);
        }
        catch (Exception creationException)
        {
            newAMQE = new QpidException(getMessage(), this);
        }

        return newAMQE;
    }

    @Override
    public QpidException clone()
    {
        return null;
    }
}
