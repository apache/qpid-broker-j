package org.apache.qpid.server.security.access.util;

import java.util.AbstractSet;
import java.util.Iterator;

public class PrefixTreeSet extends AbstractSet<String>
{
    private PrefixTree _tree = PrefixTree.empty();

    @Override
    public boolean add(String s)
    {
        _tree = _tree.mergeWith(s);
        return true;
    }

    @Override
    public Iterator<String> iterator()
    {
        return _tree.iterator();
    }

    @Override
    public int size()
    {
        return _tree.size();
    }

    public PrefixTree toPrefixTree()
    {
        return _tree;
    }
}
