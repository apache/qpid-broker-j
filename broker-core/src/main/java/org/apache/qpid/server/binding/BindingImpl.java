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
package org.apache.qpid.server.binding;

import java.util.Map;

import org.apache.qpid.server.model.Binding;

public class BindingImpl implements Binding
{
    private String _bindingKey;
    private String _destination;
    private Map<String, Object> _arguments;

    public BindingImpl(final String bindingKey,
                       final String destination,
                       final Map<String, Object> arguments)
    {
        _bindingKey = bindingKey;
        _destination = destination;
        _arguments = arguments;
    }

    @Override
    public String getName()
    {
        return getBindingKey();
    }

    @Override
    public String getType()
    {
        return TYPE;
    }

    @Override
    public String getBindingKey()
    {
        return _bindingKey;
    }

    @Override
    public String getDestination()
    {
        return _destination;
    }

    @Override
    public Map<String, Object> getArguments()
    {
        return _arguments;
    }

}
