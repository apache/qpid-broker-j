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

import com.google.common.base.Strings;
import com.google.common.collect.Iterators;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

class TreeBranch extends AbstractTreeBranch
{
    final Map<Character, AbstractTreeBranch> _branches;

    TreeBranch(String prefix)
    {
        super(prefix);
        _branches = Collections.emptyMap();
    }

    TreeBranch(Map<Character, AbstractTreeBranch> branches)
    {
        super();
        _branches = new HashMap<>(branches);
    }

    TreeBranch(String prefix, Map<Character, AbstractTreeBranch> branches)
    {
        super(prefix);
        _branches = new HashMap<>(branches);
    }

    TreeBranch(String prefix, AbstractTreeBranch first)
    {
        super(prefix);
        _branches = Collections.singletonMap(first.firstPrefixCharacter(), first);
    }

    TreeBranch(String prefix, AbstractTreeBranch first, AbstractTreeBranch second)
    {
        super(prefix);
        _branches = new HashMap<>(2);
        _branches.put(first.firstPrefixCharacter(), first);
        _branches.put(second.firstPrefixCharacter(), second);
    }

    TreeBranch(AbstractTreeBranch first, AbstractTreeBranch second)
    {
        super();
        _branches = new HashMap<>(2);
        _branches.put(first.firstPrefixCharacter(), first);
        _branches.put(second.firstPrefixCharacter(), second);
    }

    @Override
    public Map<Character, PrefixTree> branches()
    {
        return Collections.unmodifiableMap(_branches);
    }

    @Override
    public int size()
    {
        return _branches.values().stream().mapToInt(PrefixTree::size).sum();
    }

    @Override
    boolean contains(String str)
    {
        final int length = str.length();
        if (length > _length && str.startsWith(_prefix))
        {
            final String subString = str.substring(_length, length);
            final PrefixTree subTree = _branches.get(subString.charAt(0));
            if (subTree != null)
            {
                return subTree.match(subString);
            }
        }
        return false;
    }

    @Override
    public Iterator<String> iterator()
    {
        return new IteratorImpl(this);
    }

    @Override
    AbstractTreeBranch mergeString(String str)
    {
        final String common = Strings.commonPrefix(str, _prefix);
        if (common.isEmpty())
        {
            return new TreeRoot(this, new FinalBranch(str));
        }
        final int commonLength = common.length();
        if (commonLength == str.length())
        {
            if (commonLength == _length)
            {
                return newFinalBranch();
            }
            return new FinalBranch(str, newTree(_prefix.substring(commonLength), _branches));
        }
        if (commonLength == _length)
        {
            final Map<Character, AbstractTreeBranch> branches = new HashMap<>(_branches);
            final String subString = str.substring(commonLength);
            final char key = subString.charAt(0);
            final AbstractTreeBranch branch = branches.get(key);
            if (branch != null)
            {
                branches.put(key, branch.mergeString(subString));
            }
            else
            {
                branches.put(key, new FinalBranch(subString));
            }
            return newTree(common, branches);
        }

        return new TreeBranch(common,
                newTree(_prefix.substring(commonLength), _branches),
                new FinalBranch(str.substring(commonLength)));
    }

    @Override
    AbstractTreeBranch mergeWildCard(String prefix)
    {
        final String common = Strings.commonPrefix(prefix, _prefix);
        if (common.isEmpty())
        {
            return new TreeRoot(this, new WildCardBranch(prefix));
        }
        final int commonLength = common.length();
        if (commonLength == prefix.length())
        {
            return new WildCardBranch(prefix);
        }
        if (commonLength == _length)
        {
            final Map<Character, AbstractTreeBranch> branches = new HashMap<>(_branches);
            final String subString = prefix.substring(commonLength);
            final char key = subString.charAt(0);
            final AbstractTreeBranch branch = branches.get(key);
            if (branch != null)
            {
                branches.put(key, branch.mergeWildCard(subString));
            }
            else
            {
                branches.put(key, new WildCardBranch(subString));
            }
            return newTree(_prefix, branches);
        }

        return new TreeBranch(common,
                newTree(_prefix.substring(commonLength), _branches),
                new WildCardBranch(prefix.substring(commonLength)));
    }

    FinalBranch newFinalBranch()
    {
        return new FinalBranch(_prefix, _branches);
    }

    TreeBranch newTree(String prefix, Map<Character, AbstractTreeBranch> branches)
    {
        return new TreeBranch(prefix, branches);
    }

    static final class IteratorImpl implements Iterator<String>
    {
        private final String _prefix;

        private final Iterator<AbstractTreeBranch> _tree;

        private Iterator<String> _branch;

        IteratorImpl(TreeBranch root)
        {
            _prefix = root.prefix();
            _tree = new TreeMap<>(root._branches).values().iterator();
            _branch = Collections.emptyIterator();
        }

        IteratorImpl(TreeBranch root, String firstValue)
        {
            _prefix = root.prefix();
            _tree = new TreeMap<>(root._branches).values().iterator();
            _branch = Iterators.singletonIterator(firstValue);
        }

        @Override
        public boolean hasNext()
        {
            boolean hasBranch = _branch.hasNext();
            while (!hasBranch && _tree.hasNext())
            {
                _branch = _tree.next().iterator();
                hasBranch = _branch.hasNext();
            }
            return hasBranch;
        }

        @Override
        public String next()
        {
            while (!_branch.hasNext() && _tree.hasNext())
            {
                _branch = _tree.next().iterator();
            }
            return _prefix + _branch.next();
        }
    }
}
