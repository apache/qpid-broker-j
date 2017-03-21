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
 *
 */

package org.apache.qpid.util;

import java.lang.reflect.Method;

import junit.framework.TestCase;

import org.apache.qpid.AMQException;

public class AMQExceptionTestUtil
{
    private AMQExceptionTestUtil()
    {

    }

    public static void assertAMQException(final String message, final Integer expected, final AMQException e) throws Exception
    {
        // After v6.3.0, AMQException#getErrorCode() is an integer, whereas before v6.3.0, AMQException#getErrorCode() was an AMQConstant with a getCode method
        Object object = e.getErrorCode();
        final Integer errorCode;
        if (object instanceof Integer || object == null)
        {
            errorCode = (Integer) object;
        }
        else
        {
            // TODO Remove after 6.3.0 is released
            Method m = object.getClass().getMethod("getCode", new Class[]{});
            errorCode = (Integer) m.invoke(object);
        }

        TestCase.assertEquals(message, expected, errorCode);
    }
}
