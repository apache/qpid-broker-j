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
package org.apache.qpid.server.management.plugin;

import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.Plugin;
import org.apache.qpid.server.model.Port;

public interface HttpManagementConfiguration<X extends HttpManagementConfiguration<X>> extends Plugin<X>
{

    @ManagedAttribute( defaultValue = "true" )
    boolean isHttpsSaslAuthenticationEnabled();

    @ManagedAttribute( defaultValue = "true" )
    boolean isHttpSaslAuthenticationEnabled();

    @ManagedAttribute( defaultValue = "true" )
    boolean isHttpsBasicAuthenticationEnabled();

    @ManagedAttribute( defaultValue = "false" )
    boolean isHttpBasicAuthenticationEnabled();

    @ManagedAttribute( defaultValue = "600", description = "The maximum time interval, in seconds, that Web Management will keep the session open between client accesses.")
    int getSessionTimeout();

    @ManagedAttribute( defaultValue = "" )
    String getCorsAllowOrigins();

    @ManagedAttribute( defaultValue = "[\"HEAD\",\"GET\",\"POST\"]", validValues = {"org.apache.qpid.server.management.plugin.HttpManagement#getAllAvailableCorsMethodCombinations()"} )
    Set<String> getCorsAllowMethods();

    @ManagedAttribute( defaultValue = "Content-Type,Accept,Origin,X-Requested-With,X-Range" )
    String getCorsAllowHeaders();

    @ManagedAttribute( defaultValue = "true" )
    boolean getCorsAllowCredentials();

    String HTTP_MANAGEMENT_COMPRESS_RESPONSES = "httpManagement.compressResponses";
    @ManagedContextDefault(name = HTTP_MANAGEMENT_COMPRESS_RESPONSES)
    boolean DEFAULT_COMPRESS_RESPONSES = true;

    @ManagedAttribute( defaultValue = "${"+HTTP_MANAGEMENT_COMPRESS_RESPONSES+"}" )
    boolean isCompressResponses();

    @DerivedAttribute(description = "Length of time permitted for the SASL authentication exchange.")
    long getSaslExchangeExpiry();

    String MAX_HTTP_FILE_UPLOAD_SIZE_CONTEXT_NAME = "maxHttpFileUploadSize";
    @ManagedContextDefault( name = MAX_HTTP_FILE_UPLOAD_SIZE_CONTEXT_NAME)
    long DEFAULT_MAX_UPLOAD_SIZE = 100 * 1024;

    String PREFERENCE_OPERTAION_TIMEOUT_CONTEXT_NAME = "qpid.httpManagement.preferenceOperationTimeout";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = PREFERENCE_OPERTAION_TIMEOUT_CONTEXT_NAME)
    long DEFAULT_PREFERENCE_OPERATION_TIMEOUT = 10000L;

    String SASL_EXCHANGE_EXPIRY_CONTEXT_NAME = "qpid.httpManagement.saslExchangeExpiry";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = SASL_EXCHANGE_EXPIRY_CONTEXT_NAME)
    long DEFAULT_SASL_EXCHANGE_EXPIRY = 60000L;

    String DISABLE_UI_CONTEXT_NAME = "qpid.httpManagement.disableUserInterface";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = DISABLE_UI_CONTEXT_NAME)
    boolean DEFAULT_DISABLE_UI = false;

    AuthenticationProvider getAuthenticationProvider(HttpServletRequest request);
    Port<?> getPort(HttpServletRequest request);
}
