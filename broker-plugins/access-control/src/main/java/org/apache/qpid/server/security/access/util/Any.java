package org.apache.qpid.server.security.access.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.Iterators;

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
        return Iterators.singletonIterator("*");
    }
}
