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

import java.lang.reflect.Method;
import java.util.regex.Pattern;

import org.apache.qpid.server.util.ClearableCharSequence;
import org.apache.qpid.server.util.Strings;

public abstract class ConfiguredObjectMethodAttribute<C extends ConfiguredObject, T>
        extends  ConfiguredObjectMethodAttributeOrStatistic<C,T>
        implements ConfiguredObjectAttribute<C,T>
{
    ConfiguredObjectMethodAttribute(Class<C> clazz,
                                    final Method getter)
    {
        super(getter);
        if(getter.getParameterTypes().length != 0)
        {
            throw new IllegalArgumentException("ManagedAttribute annotation should only be added to no-arg getters");
        }
    }

    @Override
    public boolean isSecureValue(final Object value)
    {
        try (final ClearableCharSequence charSequence = Strings.toCharSequence(value))
        {
            final Pattern filter = getSecureValueFilter();
            return isSecure() && (filter == null || filter.matcher(charSequence).matches());
        }
    }

}
