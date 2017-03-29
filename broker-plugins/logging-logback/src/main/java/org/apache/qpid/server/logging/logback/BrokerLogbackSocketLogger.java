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

import java.util.Map;

import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedObject;

@ManagedObject( category = false, type = BrokerLogbackSocketLogger.TYPE,
                description = "Logger implementation that writes log events to a remote logback server",
                validChildTypes = "org.apache.qpid.server.logging.logback.AbstractLogger#getSupportedBrokerLoggerChildTypes()")
public interface BrokerLogbackSocketLogger<X extends BrokerLogbackSocketLogger<X>> extends BrokerLogger<X>
{
    String TYPE = "BrokerLogbackSocket";
    String PORT = "port";
    String REMOTE_HOST = "remoteHost";
    String RECONNECTION_DELAY = "reconnectionDelay";
    String INCLUDE_CALLER_DATA = "includeCallerData";
    String MAPPED_DIAGNOSTIC_CONTEXT = "mappedDiagnosticContext";
    String CONTEXT_PROPERTIES = "contextProperties";

    @ManagedAttribute(mandatory = true)
    int getPort();

    @ManagedAttribute(defaultValue = "localhost")
    String getRemoteHost();

    @ManagedAttribute(defaultValue = "100" )
    long getReconnectionDelay();

    @ManagedAttribute(defaultValue = "true")
    boolean getIncludeCallerData();

    @ManagedAttribute(defaultValue = "{}",
                      description = "The mapped diagnostic context that will accompany each logging event before being sent to the remote host.")
    Map<String, String> getMappedDiagnosticContext();

    @ManagedAttribute(defaultValue = "{}",
                      description = "Context properties that will be added to the global logback logging context.")
    Map<String, String> getContextProperties();
}
