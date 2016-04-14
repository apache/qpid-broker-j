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
import java.util.Date;

public final class ConfiguredObjectMethodStatistic<C extends ConfiguredObject, T extends Object>
        extends ConfiguredObjectMethodAttributeOrStatistic<C,T> implements ConfiguredObjectStatistic<C, T>
{
    private final ManagedStatistic _annotation;

    ConfiguredObjectMethodStatistic(Class<C> clazz, final Method getter, final ManagedStatistic annotation)
    {
        super(getter);
        _annotation = annotation;
        if(getter.getParameterTypes().length != 0)
        {
            throw new IllegalArgumentException("ManagedStatistic annotation should only be added to no-arg getters");
        }

        if(!Number.class.isAssignableFrom(getType()) && !Date.class.equals(getType()))
        {
            throw new IllegalArgumentException("ManagedStatistic annotation should only be added to getters returning a Number or Date type");
        }
    }

    @Override
    public String getDescription()
    {
        return _annotation.description();
    }

    @Override
    public StatisticUnit getUnits()
    {
        return _annotation.units();
    }

    @Override
    public StatisticType getStatisticType()
    {
        return _annotation.statisticType();
    }

    @Override
    public String getLabel()
    {
        return _annotation.label();
    }
}
