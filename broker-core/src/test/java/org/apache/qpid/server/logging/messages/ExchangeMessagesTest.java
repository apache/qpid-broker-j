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
package org.apache.qpid.server.logging.messages;

import java.util.List;

import org.junit.Test;

import org.apache.qpid.server.logging.Outcome;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.BrokerTestHelper;

/**
 * Test EXH Log Messages
 */
public class ExchangeMessagesTest extends AbstractTestMessages
{
    @Test
    public void testExchangeCreated_Transient() throws Exception
    {
        final Exchange<?> exchange = BrokerTestHelper.createExchange("test", false, getEventLogger());
        final String type = exchange.getType();
        final String name = exchange.getName();
        final String attributes =
                String.format("{createdTime=%s,durable=false,id=%s,lastUpdatedTime=%s,name=%s,type=%s}",
                              exchange.getCreatedTime().toInstant().toString(),
                              exchange.getId().toString(),
                              exchange.getLastUpdatedTime().toInstant().toString(),
                              name,
                              type);
        _logMessage = ExchangeMessages.CREATE(name, String.valueOf(Outcome.SUCCESS), attributes);
        final List<Object> log = getLog();
        final String[] expected = {"Create : \"", name, "\" : ", String.valueOf(Outcome.SUCCESS), attributes};
        validateLogMessageNoSubject(log, "EXH-1001", expected);
    }

    @Test
    public void testExchangeCreated_Persistent() throws Exception
    {
        final Exchange<?> exchange = BrokerTestHelper.createExchange("test", true, getEventLogger());
        final String type = exchange.getType();
        final String name = exchange.getName();
        final String attributes =
                String.format("{createdTime=%s,durable=true,id=%s,lastUpdatedTime=%s,name=%s,type=%s}",
                              exchange.getCreatedTime().toInstant().toString(),
                              exchange.getId().toString(),
                              exchange.getLastUpdatedTime().toInstant().toString(),
                              name,
                              type);

        _logMessage = ExchangeMessages.CREATE(name, String.valueOf(Outcome.SUCCESS), attributes);
        final List<Object> log = getLog();
        final String[] expected = {"Create : \"", name, "\" : ", String.valueOf(Outcome.SUCCESS), attributes};
        validateLogMessageNoSubject(log, "EXH-1001", expected);
    }

    @Test
    public void testExchangeDeleted()
    {
        _logMessage = ExchangeMessages.DELETE("test", String.valueOf(Outcome.SUCCESS));
        final List<Object> log = performLog();
        final String[] expected = {"Delete : \"test\" : ", String.valueOf(Outcome.SUCCESS)};

        validateLogMessage(log, "EXH-1002", expected);
    }

    @Test
    public void testExchangeDiscardedMessage() throws Exception
    {
        final Exchange<?> exchange = BrokerTestHelper.createExchange("test", false, getEventLogger());
        final String name = exchange.getName();
        final String routingKey = "routingKey";
        clearLog();
        _logMessage = ExchangeMessages.DISCARDMSG(name, routingKey);
        List<Object> log = performLog();

        String[] expected = {"Discarded Message :","Name:", "\"" + name + "\"", "Routing Key:", "\"" + routingKey + "\""};

        validateLogMessage(log, "EXH-1003", expected);
    }
}
