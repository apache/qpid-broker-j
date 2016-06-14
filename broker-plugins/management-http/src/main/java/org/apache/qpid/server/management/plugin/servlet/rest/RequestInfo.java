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
 */

package org.apache.qpid.server.management.plugin.servlet.rest;

import java.util.Collections;
import java.util.List;

public class RequestInfo
{
    private final RequestType _type;
    private final List<String> _modelParts;
    private final String _operationName;

    public RequestInfo(final RequestType type, final List<String> modelParts)
    {
        this(type, modelParts, null);
    }

    public RequestInfo(final RequestType type, final List<String> modelParts, final String operationName)
    {
        _type = type;
        _operationName = operationName;
        _modelParts = Collections.unmodifiableList(modelParts);
    }

    public RequestType getType()
    {
        return _type;
    }

    public List<String> getModelParts()
    {
        return _modelParts;
    }

    public String getOperationName()
    {
        if (_type != RequestType.OPERATION)
        {
            throw new IllegalStateException("Must not call getOperationName on non-Operation RequestInfo");
        }
        return _operationName;
    }

    enum RequestType
    {
        OPERATION, MODEL_OBJECT
    }
}
