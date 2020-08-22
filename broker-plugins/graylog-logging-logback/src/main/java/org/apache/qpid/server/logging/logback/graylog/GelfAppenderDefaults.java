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
package org.apache.qpid.server.logging.logback.graylog;

public enum GelfAppenderDefaults
{
    PORT(GelfAppenderDefaults.PORT_AS_STRING),

    RECONNECTION_INTERVAL(GelfAppenderDefaults.RECONNECTION_INTERVAL_AS_STRING),

    CONNECTION_TIMEOUT(GelfAppenderDefaults.CONNECTION_TIMEOUT_AS_STRING),

    MAXIMUM_RECONNECTION_ATTEMPTS(GelfAppenderDefaults.MAXIMUM_RECONNECTION_ATTEMPTS_AS_STRING),

    RETRY_DELAY(GelfAppenderDefaults.RETRY_DELAY_AS_STRING),

    MESSAGES_FLUSH_TIMEOUT(GelfAppenderDefaults.MESSAGES_FLUSH_TIMEOUT_AS_STRING),

    MESSAGE_BUFFER_CAPACITY(GelfAppenderDefaults.MESSAGE_BUFFER_CAPACITY_AS_STRING);

    public static final String PORT_AS_STRING = "12201";

    public static final String RECONNECTION_INTERVAL_AS_STRING = "60000";

    public static final String CONNECTION_TIMEOUT_AS_STRING = "15000";

    public static final String MAXIMUM_RECONNECTION_ATTEMPTS_AS_STRING = "2";

    public static final String RETRY_DELAY_AS_STRING = "3000";

    public static final String MESSAGES_FLUSH_TIMEOUT_AS_STRING = "1000";

    public static final String MESSAGE_BUFFER_CAPACITY_AS_STRING = "256";

    private final int _value;

    GelfAppenderDefaults(String value)
    {
        this._value = Integer.parseInt(value);
    }

    public int value()
    {
        return _value;
    }
}
