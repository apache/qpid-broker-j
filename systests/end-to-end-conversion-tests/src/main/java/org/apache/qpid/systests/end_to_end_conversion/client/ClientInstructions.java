/*
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

package org.apache.qpid.systests.end_to_end_conversion.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.systests.end_to_end_conversion.JmsInstructions;

public class ClientInstructions implements Serializable
{
    private String _contextFactory;
    private String _connectionUrl;
    private String _queueName;
    private ArrayList<JmsInstructions> _jmsInstructions;

    public String getContextFactory()
    {
        return _contextFactory;
    }

    public void setContextFactory(final String contextFactory)
    {
        _contextFactory = contextFactory;
    }

    public String getConnectionUrl()
    {
        return _connectionUrl;
    }

    public void setConnectionUrl(final String connectionUrl)
    {
        _connectionUrl = connectionUrl;
    }

    public String getQueueName()
    {
        return _queueName;
    }

    public void setQueueName(final String queueName)
    {
        _queueName = queueName;
    }

    public List<JmsInstructions> getJmsInstructions()
    {
        return _jmsInstructions;
    }

    public void setJmsInstructions(final List<JmsInstructions> jmsInstructions)
    {
        _jmsInstructions = new ArrayList<>(jmsInstructions);
    }

    @Override
    public String toString()
    {
        return "ClientInstructions{" +
               "_contextFactory='" + _contextFactory + '\'' +
               ", _connectionUrl='" + _connectionUrl + '\'' +
               ", _queueName='" + _queueName + '\'' +
               ", _jmsInstructions=" + _jmsInstructions +
               '}';
    }
}
