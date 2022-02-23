package org.apache.qpid.server.security.access.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

final class Empty extends AbstractTreeBranch
{
    static Empty INSTANCE = new Empty();

    private Empty()
    {
        super();
    }

    @Override
    public Map<Character, PrefixTree> branches()
    {
        return Collections.emptyMap();
    }

    @Override
    public int size()
    {
        return 0;
    }

    @Override
    public boolean match(String str)
    {
        return false;
    }

    @Override
    AbstractTreeBranch mergeString(String str)
    {
        return new FinalBranch(str);
    }

    @Override
    AbstractTreeBranch mergeWildCard(String prefix)
    {
        return new WildCardBranch(prefix);
    }

    @Override
    boolean contains(String str)
    {
        return false;
    }

    @Override
    public Iterator<String> iterator()
    {
        return Collections.emptyIterator();
    }
}
