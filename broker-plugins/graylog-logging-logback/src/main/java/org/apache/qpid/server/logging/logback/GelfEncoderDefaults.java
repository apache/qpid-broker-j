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
package org.apache.qpid.server.logging.logback;

public enum GelfEncoderDefaults
{
    RAW_MESSAGE_INCLUDED(GelfEncoderDefaults.RAW_MESSAGE_INCLUDED_AS_STRING),

    EVENT_MARKER_INCLUDED(GelfEncoderDefaults.EVENT_MARKER_INCLUDED_AS_STRING),

    MDC_PROPERTIES_INCLUDED(GelfEncoderDefaults.MDC_PROPERTIES_INCLUDED_AS_STRING),

    CALLER_DATA_INCLUDED(GelfEncoderDefaults.CALLER_DATA_INCLUDED_AS_STRING),

    ROOT_EXCEPTION_DATA_INCLUDED(GelfEncoderDefaults.ROOT_EXCEPTION_DATA_INCLUDED_AS_STRING),

    LOG_LEVEL_NAME_INCLUDED(GelfEncoderDefaults.LOG_LEVEL_NAME_INCLUDED_AS_STRING);

    public static final String FALSE = "false";
    public static final String TRUE = "true";

    public static final String RAW_MESSAGE_INCLUDED_AS_STRING = FALSE;

    public static final String EVENT_MARKER_INCLUDED_AS_STRING = TRUE;

    public static final String MDC_PROPERTIES_INCLUDED_AS_STRING = TRUE;

    public static final String CALLER_DATA_INCLUDED_AS_STRING = FALSE;

    public static final String ROOT_EXCEPTION_DATA_INCLUDED_AS_STRING = FALSE;

    public static final String LOG_LEVEL_NAME_INCLUDED_AS_STRING = FALSE;

    private final boolean _value;

    GelfEncoderDefaults(String value)
    {
        _value = Boolean.parseBoolean(value);
    }

    public boolean value() {
        return _value;
    }
}
