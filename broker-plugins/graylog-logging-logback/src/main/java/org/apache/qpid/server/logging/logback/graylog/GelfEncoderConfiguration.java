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

import java.util.Collections;
import java.util.Map;

public interface GelfEncoderConfiguration
{
    String getMessageOriginHost();

    default boolean isRawMessageIncluded()
    {
        return GelfEncoderDefaults.RAW_MESSAGE_INCLUDED.value();
    }

    default boolean isEventMarkerIncluded()
    {
        return GelfEncoderDefaults.EVENT_MARKER_INCLUDED.value();
    }

    default boolean hasMdcPropertiesIncluded()
    {
        return GelfEncoderDefaults.MDC_PROPERTIES_INCLUDED.value();
    }

    default boolean isCallerDataIncluded()
    {
        return GelfEncoderDefaults.CALLER_DATA_INCLUDED.value();
    }

    default boolean hasRootExceptionDataIncluded()
    {
        return GelfEncoderDefaults.ROOT_EXCEPTION_DATA_INCLUDED.value();
    }

    default boolean isLogLevelNameIncluded()
    {
        return GelfEncoderDefaults.LOG_LEVEL_NAME_INCLUDED.value();
    }

    default Map<String, Object> getStaticFields()
    {
        return Collections.emptyMap();
    }
}
