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
package org.apache.qpid.systests.jms_1_1.extensions.connectionlimit;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.naming.NamingException;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.user.connection.limits.plugins.ConnectionLimitRule;
import org.apache.qpid.systests.JmsTestBase;

public class MessagingConnectionLimitTest extends JmsTestBase
{
    private static final String USER = "admin";
    private static final String USER_SECRET = "admin";
    private static final String RULE_BASED_VIRTUAL_HOST_USER_CONNECTION_LIMIT_PROVIDER =
            "org.apache.qpid.RuleBasedVirtualHostConnectionLimitProvider";
    private static final String FREQUENCY_PERIOD = "frequencyPeriod";
    private static final String RULES = "rules";

    @Test
    public void testAuthorizationWithConnectionLimit() throws Exception
    {
        final int connectionLimit = 2;
        configureCLT(new ConnectionLimitRule()
        {
            @Override
            public String getPort()
            {
                return null;
            }

            @Override
            public String getIdentity()
            {
                return USER;
            }

            @Override
            public Boolean getBlocked()
            {
                return Boolean.FALSE;
            }

            @Override
            public Integer getCountLimit()
            {
                return connectionLimit;
            }

            @Override
            public Integer getFrequencyLimit()
            {
                return null;
            }

            @Override
            public Long getFrequencyPeriod()
            {
                return null;
            }
        });

        final List<Connection> establishedConnections = new ArrayList<>();
        try
        {
            establishConnections(connectionLimit, establishedConnections);
            verifyConnectionEstablishmentFails(connectionLimit);
            establishedConnections.remove(0).close();
            establishConnection(establishedConnections, connectionLimit);
        }
        finally
        {
            closeConnections(establishedConnections);
        }
    }

    @Test
    public void testAuthorizationWithConnectionFrequencyLimit() throws Exception
    {
        final int connectionFrequencyLimit = 1;
        configureCLT(new ConnectionLimitRule()
        {
            @Override
            public String getPort()
            {
                return null;
            }

            @Override
            public String getIdentity()
            {
                return USER;
            }

            @Override
            public Boolean getBlocked()
            {
                return Boolean.FALSE;
            }

            @Override
            public Integer getCountLimit()
            {
                return null;
            }

            @Override
            public Integer getFrequencyLimit()
            {
                return connectionFrequencyLimit;
            }

            @Override
            public Long getFrequencyPeriod()
            {
                return 60L * 1000L;
            }
        });

        final List<Connection> establishedConnections = new ArrayList<>();
        try
        {
            establishConnections(connectionFrequencyLimit, establishedConnections);
            verifyConnectionEstablishmentFails(connectionFrequencyLimit);
            establishedConnections.remove(0).close();
            verifyConnectionEstablishmentFails(connectionFrequencyLimit);
        }
        finally
        {
            closeConnections(establishedConnections);
        }
    }

    @Test
    public void testAuthorizationWithConnectionLimitAndFrequencyLimit() throws Exception
    {
        final int connectionFrequencyLimit = 2;
        final int connectionLimit = 3;
        configureCLT(new ConnectionLimitRule()
        {
            @Override
            public String getPort()
            {
                return null;
            }

            @Override
            public String getIdentity()
            {
                return USER;
            }

            @Override
            public Boolean getBlocked()
            {
                return Boolean.FALSE;
            }

            @Override
            public Integer getCountLimit()
            {
                return connectionLimit;
            }

            @Override
            public Integer getFrequencyLimit()
            {
                return connectionFrequencyLimit;
            }

            @Override
            public Long getFrequencyPeriod()
            {
                return 60L * 1000L;
            }
        });

        final List<Connection> establishedConnections = new ArrayList<>();
        try
        {
            establishConnections(connectionFrequencyLimit, establishedConnections);
            verifyConnectionEstablishmentFails(connectionFrequencyLimit);
            establishedConnections.remove(0).close();
            verifyConnectionEstablishmentFails(connectionFrequencyLimit);
        }
        finally
        {
            closeConnections(establishedConnections);
        }
    }

    @Test
    public void testAuthorizationWithBlockedUser() throws Exception
    {
        configureCLT(new ConnectionLimitRule()
        {
            @Override
            public String getPort()
            {
                return null;
            }

            @Override
            public String getIdentity()
            {
                return USER;
            }

            @Override
            public Boolean getBlocked()
            {
                return Boolean.TRUE;
            }

            @Override
            public Integer getCountLimit()
            {
                return null;
            }

            @Override
            public Integer getFrequencyLimit()
            {
                return null;
            }

            @Override
            public Long getFrequencyPeriod()
            {
                return null;
            }
        });
        verifyConnectionEstablishmentFails(0);
    }

    private void establishConnections(final int connectionNumber, final List<Connection> establishedConnections)
            throws NamingException, JMSException
    {
        for (int i = 0; i < connectionNumber; i++)
        {
            establishConnection(establishedConnections, i);
        }
    }

    private void establishConnection(List<Connection> establishedConnections, int index)
            throws NamingException, JMSException
    {
        establishedConnections.add(getConnectionBuilder().setUsername(USER)
                .setPassword(USER_SECRET)
                .setClientId(getTestName() + index)
                .build());
    }

    private void closeConnections(final List<Connection> establishedConnections) throws JMSException
    {
        for (final Connection c : establishedConnections)
        {
            try
            {
                c.close();
            }
            catch (RuntimeException e)
            {
                // Close all connections
            }
        }
    }

    private void verifyConnectionEstablishmentFails(final int frequencyLimit) throws NamingException
    {
        try
        {
            final Connection connection = getConnectionBuilder().setUsername(USER)
                    .setPassword(USER_SECRET)
                    .setClientId(getTestName() + frequencyLimit)
                    .build();

            connection.close();
            fail("Connection creation should fail due to exceeding limit");
        }
        catch (JMSException e)
        {
            //pass
        }
    }

    private void configureCLT(ConnectionLimitRule... rules) throws Exception
    {
        final String serializedRules = new ObjectMapper().writeValueAsString(rules);
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(RULES, serializedRules);
        attributes.put(FREQUENCY_PERIOD, "60000");
        createEntityUsingAmqpManagement("clt", RULE_BASED_VIRTUAL_HOST_USER_CONNECTION_LIMIT_PROVIDER, attributes);
    }
}
