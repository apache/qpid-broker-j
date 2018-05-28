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
 *
 *
 */
package org.apache.qpid.server.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.filter.selector.ParseException;
import org.apache.qpid.server.filter.selector.SelectorParser;
import org.apache.qpid.server.filter.selector.TokenMgrError;
import org.apache.qpid.server.plugin.PluggableService;


@PluggableService
public class JMSSelectorFilter implements MessageFilter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JMSSelectorFilter.class);

    private String _selector;
    private BooleanExpression _matcher;

    public JMSSelectorFilter(String selector) throws ParseException, TokenMgrError, SelectorParsingException
    {
        _selector = selector;
        SelectorParser<FilterableMessage> selectorParser = new SelectorParser<>();
        selectorParser.setPropertyExpressionFactory(JMSMessagePropertyExpression.FACTORY);
        _matcher = selectorParser.parse(selector);
    }

    @Override
    public String getName()
    {
        return AMQPFilterTypes.JMS_SELECTOR.toString();
    }

    @Override
    public boolean matches(Filterable message)
    {

        boolean match = _matcher.matches(message);
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug(message + " match(" + match + ") selector(" + System.identityHashCode(_selector) + "):" + _selector);
        }
        return match;
    }

    @Override
    public boolean startAtTail()
    {
        return false;
    }

    public String getSelector()
    {
        return _selector;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final JMSSelectorFilter that = (JMSSelectorFilter) o;

        return getSelector().equals(that.getSelector());

    }

    @Override
    public int hashCode()
    {
        return getSelector().hashCode();
    }

    @Override
    public String toString()
    {
        return "JMSSelectorFilter[" +
               "selector='" + _selector + '\'' +
               ']';
    }
}
