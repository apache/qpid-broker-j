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

import java.util.Objects;

abstract class AbstractTreeBranch implements PrefixTree
{
    private static final String NO_PREFIX = "Tree root does not have a prefix";
    private static final String EMPTY_INPUT_PREFIX = "The input prefix can not be empty";
    private static final String EMPTY_INPUT_STRING = "The input string can not be empty";

    final String _prefix;

    final int _length;

    abstract AbstractTreeBranch mergeString(String str);

    abstract AbstractTreeBranch mergeWildCard(String prefix);

    abstract boolean contains(String str);

    AbstractTreeBranch(String prefix)
    {
        super();
        _prefix = Objects.requireNonNull(prefix, "The prefix can not be null!");
        _length = _prefix.length();
    }

    AbstractTreeBranch()
    {
        super();
        _prefix = "";
        _length = 0;
    }

    @Override
    public String prefix()
    {
        return _prefix;
    }

    @Override
    public char firstPrefixCharacter()
    {
        if (_prefix.isEmpty())
        {
            throw new UnsupportedOperationException(NO_PREFIX);
        }
        return _prefix.charAt(0);
    }

    @Override
    public PrefixTree mergeWithFinalValue(String str)
    {
        if (str == null || str.isEmpty())
        {
            throw new IllegalArgumentException(EMPTY_INPUT_STRING);
        }
        return mergeString(str);
    }

    @Override
    public PrefixTree mergeWithPrefix(String prefix)
    {
        if (prefix == null || prefix.isEmpty())
        {
            throw new IllegalArgumentException(EMPTY_INPUT_PREFIX);
        }
        return mergeWildCard(prefix);
    }

    @Override
    public boolean match(String str)
    {
        if (str == null || str.length() == 0)
        {
            return false;
        }
        return contains(str);
    }
}
