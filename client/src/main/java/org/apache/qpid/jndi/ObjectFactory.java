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
package org.apache.qpid.jndi;

import static org.apache.qpid.client.AMQConnection.JNDI_ADDRESS_CONNECTION_URL;
import static org.apache.qpid.client.PooledConnectionFactory.JNDI_ADDRESS_MAX_POOL_SIZE;
import static org.apache.qpid.client.PooledConnectionFactory.JNDI_ADDRESS_CONNECTION_TIMEOUT;
import static org.apache.qpid.client.AMQDestination.JNDI_ADDRESS_DESTINATION_ADDRESS;

import java.net.URISyntaxException;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQQueue;
import org.apache.qpid.client.AMQTopic;
import org.apache.qpid.client.PooledConnectionFactory;
import org.apache.qpid.messaging.Address;
import org.apache.qpid.url.AMQBindingURL;
import org.apache.qpid.url.URLSyntaxException;

public class ObjectFactory implements javax.naming.spi.ObjectFactory
{
    @Override
    public Object getObjectInstance(Object object, Name name, Context nameCtx, Hashtable<?, ?> environment) throws Exception
    {
        if (object instanceof Reference)
        {
            Reference reference = (Reference)object;
            String referenceClassName = reference.getClassName();

            if (referenceClassName.equals(AMQConnectionFactory.class.getName()))
            {
                return createAMQConnectionFactory(reference);
            }
            else if (referenceClassName.equals(PooledConnectionFactory.class.getName()))
            {
                return createPooledConnectionFactory(reference);
            }
            else if (referenceClassName.equals(AMQConnection.class.getName()))
            {
                return createAMQConnection(reference);
            }
            else if (referenceClassName.equals(AMQQueue.class.getName()))
            {
                return createAMQQueue(reference);
            }
            else if (referenceClassName.equals(AMQTopic.class.getName()))
            {
                return createAMQTopic(reference);
            }

        }
        return null;
    }

    private PooledConnectionFactory createPooledConnectionFactory(Reference reference) throws URLSyntaxException
    {
        Object connectionURL = getRefAddressContent(reference, JNDI_ADDRESS_CONNECTION_URL);
        if (connectionURL instanceof String)
        {
            PooledConnectionFactory connectionFactory = new PooledConnectionFactory();
            connectionFactory.setConnectionURLString(String.valueOf(connectionURL));

            Object maxPoolSize = getRefAddressContent(reference, JNDI_ADDRESS_MAX_POOL_SIZE);
            if (maxPoolSize != null)
            {
                connectionFactory.setMaxPoolSize(maxPoolSize instanceof Number ? ((Number) maxPoolSize).intValue() : Integer.parseInt(String.valueOf(maxPoolSize)));
            }

            Object connectionTimeout = getRefAddressContent(reference, JNDI_ADDRESS_CONNECTION_TIMEOUT);
            if (connectionTimeout != null)
            {
                connectionFactory.setConnectionTimeout(connectionTimeout instanceof Number ? ((Number) connectionTimeout).longValue() : Long.parseLong(String.valueOf(connectionURL)));
            }

            return connectionFactory;
        }

        return null;
    }

    private AMQConnection createAMQConnection(Reference reference) throws URLSyntaxException, QpidException
    {
        Object connectionURL = getRefAddressContent(reference, JNDI_ADDRESS_CONNECTION_URL);

        if (connectionURL == null)
        {
            // for backward compatibility
            connectionURL = getRefAddressContent(reference, AMQConnection.class.getName());
        }

        if (connectionURL instanceof String)
        {
            return new AMQConnection((String) connectionURL);
        }

        return null;
    }

    private AMQTopic createAMQTopic(Reference reference) throws URISyntaxException
    {
        return (AMQTopic)createAMQDestination(reference, AMQTopic.class);
    }

    private AMQQueue createAMQQueue(Reference reference) throws URISyntaxException
    {
        return (AMQQueue)createAMQDestination(reference, AMQQueue.class);
    }

    private AMQConnectionFactory createAMQConnectionFactory(Reference reference) throws URLSyntaxException
    {
        Object connectionURL = getRefAddressContent(reference, JNDI_ADDRESS_CONNECTION_URL);

        if (connectionURL == null)
        {
            // for backward compatibility
            connectionURL = getRefAddressContent(reference, AMQConnectionFactory.class.getName());
        }

        if (connectionURL instanceof String)
        {
            return new AMQConnectionFactory((String) connectionURL);
        }

        return null;
    }

    private Object getRefAddressContent(Reference reference, String addressType)
    {
        RefAddr refAddr = reference.get(addressType);
        if (refAddr != null)
        {
            return refAddr.getContent();
        }
        return null;
    }

    private AMQDestination createAMQDestination(Reference reference, Class<? extends AMQDestination> destinationClass) throws URISyntaxException
    {
        AMQDestination.DestSyntax addressSyntax = AMQDestination.DestSyntax.ADDR;

        Object address = getRefAddressContent(reference, JNDI_ADDRESS_DESTINATION_ADDRESS);
        if (address == null)
        {
            // for backward compatibility
            address = getRefAddressContent(reference, destinationClass.getName());
            if (address instanceof String)
            {
                addressSyntax = AMQDestination.DestSyntax.BURL;
            }
        }

        if (address instanceof String)
        {
            String addressString = (String)address;
            if (addressString.startsWith(AMQDestination.DestSyntax.BURL.asPrefix()))
            {
                addressSyntax = AMQDestination.DestSyntax.BURL;
                addressString = addressString.substring(AMQDestination.DestSyntax.BURL.asPrefix().length());
            }
            else if (addressString.startsWith(AMQDestination.DestSyntax.ADDR.asPrefix()))
            {
                addressSyntax = AMQDestination.DestSyntax.ADDR;
                addressString = addressString.substring(AMQDestination.DestSyntax.BURL.asPrefix().length());
            }

            return createAMQDestination(destinationClass, addressSyntax, addressString);
        }

        return null;
    }

    private AMQDestination createAMQDestination(Class<? extends AMQDestination> destinationClass, AMQDestination.DestSyntax addressSyntax, String addressString) throws URISyntaxException
    {
        if (destinationClass == AMQQueue.class)
        {
            if (addressSyntax == AMQDestination.DestSyntax.ADDR)
            {
                return new AMQQueue(Address.parse(addressString));
            }
            else
            {
                return new AMQQueue(new AMQBindingURL(addressString));
            }
        }
        else if (destinationClass == AMQTopic.class)
        {
            if (addressSyntax == AMQDestination.DestSyntax.ADDR)
            {
                return new AMQTopic(Address.parse(addressString));
            }
            else
            {
                return new AMQTopic(new AMQBindingURL(addressString));
            }
        }
        else
        {
            throw new IllegalStateException("Unsupported destination " + destinationClass);
        }
    }

}
