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
package org.apache.qpid.server.model;

import java.util.Map;

public abstract class AbstractGroupProvider<X extends AbstractGroupProvider<X>>
        extends AbstractConfiguredObject<X> implements GroupProvider<X>
{
    private final Container<?> _container;

    @ManagedAttributeField
    private boolean _caseSensitive;

    protected AbstractGroupProvider(Container<?> container, Map<String, Object> attributes)
    {
        super(container, attributes);
        _container = container;
    }

    @Override
    public boolean isCaseSensitive()
    {
        return _caseSensitive;
    }
}
