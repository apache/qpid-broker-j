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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

final class Any extends AbstractTreeBranch
{
    static Any INSTANCE = new Any();

    private Any()
    {
        super();
    }

    @Override
    AbstractTreeBranch mergeString(String str)
    {
        return this;
    }

    @Override
    AbstractTreeBranch mergeWildCard(String prefix)
    {
        return this;
    }

    @Override
    public boolean match(String str)
    {
        return str != null;
    }

    @Override
    boolean contains(String str)
    {
        return true;
    }

    @Override
    public Map<Character, PrefixTree> branches()
    {
        return Collections.emptyMap();
    }

    @Override
    public int size()
    {
        return 1;
    }

    @Override
    public Iterator<String> iterator()
    {
        return Stream.of("*").iterator();
    }
}
