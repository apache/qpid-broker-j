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

package org.apache.qpid.test.utils;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.NamingException;

import org.apache.qpid.url.URLSyntaxException;

public interface ConnectionBuilder
{
    ConnectionBuilder setHost(String host);
    ConnectionBuilder setPort(int port);
    ConnectionBuilder setSslPort(int port);
    ConnectionBuilder setPrefetch(int prefetch);
    ConnectionBuilder setClientId(String clientId);
    ConnectionBuilder setUsername(String username);
    ConnectionBuilder setPassword(String password);
    ConnectionBuilder setVirtualHost(String virtualHostName);
    ConnectionBuilder setFailover(boolean enableFailover);
    ConnectionBuilder setFailoverReconnectAttempts(int reconnectAttempts);
    ConnectionBuilder setTls(boolean enableTls);
    ConnectionBuilder setSyncPublish(boolean syncPublish);

    Connection build() throws NamingException, JMSException, URLSyntaxException;
    ConnectionFactory buildConnectionFactory() throws NamingException, URLSyntaxException;
}
