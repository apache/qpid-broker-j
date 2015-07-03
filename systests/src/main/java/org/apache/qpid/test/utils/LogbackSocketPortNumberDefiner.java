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

package org.apache.qpid.test.utils;

import ch.qos.logback.core.PropertyDefinerBase;

public class LogbackSocketPortNumberDefiner extends PropertyDefinerBase
{
    /**
     * Port number that will be bound by a Logback Socket Receiver.  This is assigned once per JVM instance.
     */
    private static final int LOGBACK_SOCKET_PORT_NUMBER = new PortHelper().getNextAvailable(Integer.getInteger("qpid.logback.receiver.port", 15000));

    public static int getLogbackSocketPortNumber()
    {
        return LOGBACK_SOCKET_PORT_NUMBER;
    }

    @Override
    public String getPropertyValue()
    {
        return String.valueOf(LOGBACK_SOCKET_PORT_NUMBER);
    }
}
