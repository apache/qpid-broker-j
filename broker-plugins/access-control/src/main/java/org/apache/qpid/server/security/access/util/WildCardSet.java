/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.qpid.server.security.access.util;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;

import org.apache.qpid.server.security.access.config.AclRulePredicatesBuilder;

import com.google.common.collect.Iterators;

public class WildCardSet extends AbstractSet<Object>
{
    private static final WildCardSet INSTANCE = new WildCardSet();

    public static WildCardSet newSet()
    {
        return INSTANCE;
    }

    private WildCardSet()
    {
        super();
    }

    @Override
    public boolean add(Object o)
    {
        return false;
    }

    @Override
    public boolean addAll(Collection<?> c)
    {
        return false;
    }

    @Override
    public Iterator<Object> iterator()
    {
        return Iterators.singletonIterator(AclRulePredicatesBuilder.WILD_CARD);
    }

    @Override
    public int size()
    {
        return 1;
    }
}
