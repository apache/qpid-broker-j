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
package org.apache.qpid.server.management.plugin.controller;

import java.util.Collections;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.ResponseType;

public class ControllerManagementResponse implements ManagementResponse
{
    private final ResponseType _type;
    private final Object _body;
    private final int _status;
    private final Map<String, String> _headers;

    public ControllerManagementResponse(final ResponseType type, final Object body)
    {
        this(type, body, HttpServletResponse.SC_OK, Collections.emptyMap());
    }

    public ControllerManagementResponse(final ResponseType type,
                                        final Object body,
                                        final int status,
                                        final Map<String, String> headers)
    {
        _type = type;
        _body = body;
        _status = status;
        _headers = headers;
    }

    @Override
    public ResponseType getType()
    {
        return _type;
    }

    @Override
    public Object getBody()
    {
        return _body;
    }

    public int getStatus()
    {
        return _status;
    }

    @Override
    public Map<String, String> getHeaders()
    {
        return _headers;
    }

    @Override
    public int getResponseCode()
    {
        return _status;
    }
}
