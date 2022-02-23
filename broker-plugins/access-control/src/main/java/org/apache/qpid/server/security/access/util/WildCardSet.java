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
