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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.test.utils.UnitTestBase;


/**
 * This test is to ensure that when an AMQException is rethrown that the specified exception is correctly wrapped up.
 * <p>
 * There are three cases:
 * Re-throwing an AMQException
 * Re-throwing a Subclass of AMQException
 * Re-throwing a Subclass of AMQException that does not have the default AMQException constructor which will force the
 * creation of an AMQException.
 */
public class QpidExceptionTest extends UnitTestBase
{
    /**
     * Test that an AMQException will be correctly created and rethrown.
     */
    @Test
    public void testRethrowGeneric()
    {
        QpidException test = new QpidException("refused", new RuntimeException());

        QpidException e = reThrowException(test);

        assertEquals("Exception not of correct class", QpidException.class, e.getClass());
    }

    /**
     * Test that a subclass of AMQException that has the default constructor will be correctly created and rethrown.
     */
    @Test
    public void testRethrowAMQESubclass()
    {
        AMQFrameDecodingException test = new AMQFrameDecodingException(
                "Error",
                                                                       new Exception());
        QpidException e = reThrowException(test);

        assertEquals("Exception not of correct class", AMQFrameDecodingException.class, e.getClass());
    }

    /**
     * Test that a subclass of AMQException that doesnot have the  default constructor will be correctly rethrown as an
     * AMQException
     */
    @Test
    public void testRethrowAMQESubclassNoConstructor()
    {
        AMQExceptionSubclass test = new AMQExceptionSubclass("Invalid Argument Exception");

        QpidException e = reThrowException(test);

        assertEquals("Exception not of correct class", QpidException.class, e.getClass());
    }

    /**
     * Private method to rethrown and validate the basic values of the rethrown
     * @param test Exception to rethrow
     * @throws QpidException the rethrown exception
     */
    private QpidException reThrowException(QpidException test)
    {
        QpidException amqe = test.cloneForCurrentThread();
        if(test instanceof AMQException)
        {
            assertEquals("Error code does not match.",
                                (long) ((AMQException) test).getErrorCode(),
                                (long) ((AMQException) amqe).getErrorCode());

        }
        assertTrue("Exception message does not start as expected.",
                          amqe.getMessage().startsWith(test.getMessage()));
        assertEquals("Test Exception is not set as the cause", test, amqe.getCause());
        assertEquals("Cause is not correct", test.getCause(), amqe.getCause().getCause());

        return amqe;
    }

    @Test
    public void testGetMessageAsString()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 25; i++)
        {
            sb.append("message [" + i + "]");
        }
        AMQException e = new AMQException(ErrorCodes.INTERNAL_ERROR, sb.toString(), null);
        AMQShortString message = AMQShortString.validValueOf(e.getMessage());
        assertEquals(sb.substring(0, AMQShortString.MAX_LENGTH - 3) + "...", message.toString());
    }

    /**
     * Private class that extends AMQException but does not have a default exception.
     */
    private class AMQExceptionSubclass extends QpidException
    {

        public AMQExceptionSubclass(String msg)
        {
            super(msg, null);
        }
    }
}

