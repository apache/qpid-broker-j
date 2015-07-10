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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.apache.qpid.client.AMQConnection.JNDI_ADDRESS_CONNECTION_URL;
import static org.apache.qpid.client.AMQDestination.JNDI_ADDRESS_DESTINATION_ADDRESS;
import static org.apache.qpid.client.PooledConnectionFactory.JNDI_ADDRESS_CONNECTION_TIMEOUT;
import static org.apache.qpid.client.PooledConnectionFactory.JNDI_ADDRESS_MAX_POOL_SIZE;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;

import junit.framework.TestCase;
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQQueue;
import org.apache.qpid.client.AMQTopic;
import org.apache.qpid.client.PooledConnectionFactory;
import org.apache.qpid.messaging.Address;
import org.apache.qpid.test.utils.QpidTestCase;

public class ObjectFactoryTest extends QpidTestCase
{
    private static final String TEST_CONNECTION_URL = "amqp://:@clientid/test?brokerlist='tcp://localhost:5672'";
    private static final String TEST_QUEUE_BINDING_URL = "direct://amq.direct/myQueue/myQueue?routingkey='myQueue'&durable='true'";
    private static final String TEST_TOPIC_BINDING_URL = "topic://amq.topic/myTopic/myTopic?routingkey='myTopic'&exclusive='true'&autodelete='true'";
    private static final String TEST_QUEUE_ADDRESS = "myQueue; {create: always, node:{ type: queue }}";
    private static final String TEST_TOPIC_ADDRESS = "myTopic; {create: always, node:{ type: topic }}";

    private ObjectFactory _objectFactory;
    private Name _name;
    private Context _context;
    private Hashtable<?, ?> _environment;

    public void setUp() throws Exception
    {
        super.setUp();
        _objectFactory = new ObjectFactory();

        _name = mock(Name.class);
        _context = mock(Context.class);
        _environment = mock(Hashtable.class);
    }

    public void testGetObjectInstanceReturnsNullIfNotReference() throws Exception
    {
        Object factory = _objectFactory.getObjectInstance(new Object(), _name, _context, _environment);
        assertNull("Null should be returned for non Reference object", factory);
    }

    public void testGetObjectInstanceReturnsNullForUnsupportedClassName() throws Exception
    {
        Reference reference = createMockReference(Object.class.getName(), "test", "test");

        Object factory = _objectFactory.getObjectInstance(reference, _name, _context, _environment);
        assertNull("Null should be returned for unsupported class name", factory);
    }

    public void testGetObjectInstanceReturnsNullForSupportedClassButUnsupportedAddress() throws Exception
    {
        Reference reference = createMockReference(AMQConnectionFactory.class.getName(), "test", 1);

        Object factory = _objectFactory.getObjectInstance(reference, _name, _context, _environment);
        assertNull("Null should be returned for unsupported content", factory);
    }

    public void testGetObjectInstanceOfAMQConnectionFactoryUsingLegacyAddress() throws Exception
    {
        Reference reference = createMockReference(AMQConnectionFactory.class.getName(),
                AMQConnectionFactory.class.getName(), TEST_CONNECTION_URL);

        Object factory = _objectFactory.getObjectInstance(reference, _name, _context, _environment);
        assertTrue("Unexpected object type : " + factory , factory instanceof AMQConnectionFactory);
        assertEquals("Unexpected connection URL", new AMQConnectionURL(TEST_CONNECTION_URL),
                ((AMQConnectionFactory) factory).getConnectionURL());
    }

    public void testGetObjectInstanceOfAMQConnectionFactoryUsingConnectionURLAddress() throws Exception
    {
        Reference reference = createMockReference(AMQConnectionFactory.class.getName(),
                JNDI_ADDRESS_CONNECTION_URL, TEST_CONNECTION_URL);

        Object factory = _objectFactory.getObjectInstance(reference, _name, _context, _environment);
        assertTrue("Unexpected object type : " + factory , factory instanceof AMQConnectionFactory);
        TestCase.assertEquals("Unexpected connection URL", new AMQConnectionURL(TEST_CONNECTION_URL),
                ((AMQConnectionFactory) factory).getConnectionURL());
    }

    public void testGetObjectInstanceOfAMQQueueUsingLegacyAddress() throws Exception
    {
        Reference reference = createMockReference(AMQQueue.class.getName(), AMQQueue.class.getName(), TEST_QUEUE_BINDING_URL);

        Object queue = _objectFactory.getObjectInstance(reference, _name, _context, _environment);
        assertTrue("Unexpected object type : " + queue, queue instanceof AMQQueue);
        assertEquals("Unexpected binding URL", TEST_QUEUE_BINDING_URL, ((AMQQueue) queue).toURL());
    }

    public void testGetObjectInstanceOfAMQQueueUsingBindingUrlAddress() throws Exception
    {
        Reference reference = createMockReference(AMQQueue.class.getName(), JNDI_ADDRESS_DESTINATION_ADDRESS,
                AMQDestination.DestSyntax.BURL.asPrefix() + TEST_QUEUE_BINDING_URL);

        Object queue = _objectFactory.getObjectInstance(reference, _name, _context, _environment);
        assertTrue("Unexpected object type : " + queue , queue instanceof AMQQueue);
        assertEquals("Unexpected binding URL", TEST_QUEUE_BINDING_URL, ((AMQQueue) queue).toURL());
    }

    public void testGetObjectInstanceOfAMQQueueUsingAddressBasedAddress() throws Exception
    {
        Reference reference = createMockReference(AMQQueue.class.getName(), JNDI_ADDRESS_DESTINATION_ADDRESS,
                TEST_QUEUE_ADDRESS);

        Object queue = _objectFactory.getObjectInstance(reference, _name, _context, _environment);
        assertTrue("Unexpected object type : " + queue, queue instanceof AMQQueue);
        assertTrue("Unexpected address " + ((AMQQueue) queue).getAddress(),
                equals(Address.parse(TEST_QUEUE_ADDRESS), ((AMQQueue) queue).getAddress()));
    }

    public void testGetObjectInstanceOfAMQQueueUsingPrefixedAddressBasedAddress() throws Exception
    {
        Reference reference = createMockReference(AMQQueue.class.getName(), JNDI_ADDRESS_DESTINATION_ADDRESS,
                AMQDestination.DestSyntax.ADDR.asPrefix() + TEST_QUEUE_ADDRESS);

        Object queue = _objectFactory.getObjectInstance(reference, _name, _context, _environment);
        assertTrue("Unexpected object type : " + queue, queue instanceof AMQQueue);
        assertTrue("Unexpected address " + ((AMQQueue) queue).getAddress(),
                equals(Address.parse(TEST_QUEUE_ADDRESS), ((AMQQueue) queue).getAddress()));
    }

    public void testGetObjectInstanceOfAMQTopicUsingLegacyAddressType() throws Exception
    {
        Reference reference = createMockReference(AMQTopic.class.getName(), AMQTopic.class.getName(), TEST_TOPIC_BINDING_URL);

        Object topic = _objectFactory.getObjectInstance(reference, _name, _context, _environment);
        assertTrue("Unexpected object type : " + topic , topic instanceof AMQTopic);
        assertEquals("Unexpected binding URL", TEST_TOPIC_BINDING_URL, ((AMQTopic) topic).toURL());
    }

    public void testGetObjectInstanceOfAMQTopicUsingBindingURLAddress() throws Exception
    {
        Reference reference = createMockReference(AMQTopic.class.getName(), JNDI_ADDRESS_DESTINATION_ADDRESS,
                AMQDestination.DestSyntax.BURL.asPrefix() + TEST_TOPIC_BINDING_URL);

        Object queue = _objectFactory.getObjectInstance(reference, _name, _context, _environment);
        assertTrue("Unexpected object type : " + queue , queue instanceof AMQTopic);
        assertEquals("Unexpected binding URL", TEST_TOPIC_BINDING_URL, ((AMQTopic) queue).toURL());
    }

    public void testGetObjectInstanceOfAMQTopicUsingAddressBasedAddress() throws Exception
    {
        Reference reference = createMockReference(AMQTopic.class.getName(), JNDI_ADDRESS_DESTINATION_ADDRESS,
                TEST_TOPIC_ADDRESS);

        Object queue = _objectFactory.getObjectInstance(reference, _name, _context, _environment);
        assertTrue("Unexpected object type : " + queue , queue instanceof AMQTopic);
        assertTrue("Unexpected address " + ((AMQTopic) queue).getAddress(),
                equals(Address.parse(TEST_TOPIC_ADDRESS), ((AMQTopic) queue).getAddress()));
    }

    public void testGetObjectInstanceOfAMQTopicUsingPrefixedAddressBasedAddress() throws Exception
    {
        Reference reference = createMockReference(AMQTopic.class.getName(), JNDI_ADDRESS_DESTINATION_ADDRESS,
                AMQDestination.DestSyntax.ADDR.asPrefix() + TEST_TOPIC_ADDRESS);

        Object queue = _objectFactory.getObjectInstance(reference, _name, _context, _environment);
        assertTrue("Unexpected object type : " + queue , queue instanceof AMQTopic);
        assertTrue("Unexpected address " + ((AMQTopic) queue).getAddress(),
                equals(Address.parse(TEST_TOPIC_ADDRESS), ((AMQTopic) queue).getAddress()));
    }

    public void testGetObjectInstanceOfAMQConnectionUsingLegacyAddressType() throws Exception
    {
        Reference reference = createMockReference(AMQConnection.class.getName(), AMQConnection.class.getName(), TEST_CONNECTION_URL + "&failover='unsupported'");

        // in order to prevent establishing of TCP connection
        // testing AMQConnection creation indirectly relying on IAE thrown for unsupported failover method
        try
        {
            _objectFactory.getObjectInstance(reference, _name, _context, _environment);
            fail("Exception is expected");
        }
        catch(IllegalArgumentException e)
        {
            assertEquals("Unknown failover method:unsupported", e.getMessage());
        }
    }

    public void testGetObjectInstanceOfAMQConnectionUsingAddressTypeAddress() throws Exception
    {
        Reference reference = createMockReference(AMQConnection.class.getName(), JNDI_ADDRESS_CONNECTION_URL, TEST_CONNECTION_URL + "&failover='unsupported'");

        // in order to prevent establishing of TCP connection
        // testing AMQConnection creation indirectly relying on IAE thrown for unsupported failover method
        try
        {
            _objectFactory.getObjectInstance(reference, _name, _context, _environment);
            fail("Exception is expected");
        }
        catch(IllegalArgumentException e)
        {
            assertEquals("Unknown failover method:unsupported", e.getMessage());
        }
    }

    public void testGetObjectInstanceOfPooledConnectionFactory() throws Exception
    {
        Reference reference = createMockReference(PooledConnectionFactory.class.getName(),
                JNDI_ADDRESS_CONNECTION_URL, TEST_CONNECTION_URL);

        Name name = mock(Name.class);
        Context context = mock(Context.class);
        Hashtable<?, ?> environment = mock(Hashtable.class);

        Object factory = _objectFactory.getObjectInstance(reference, name, context, environment);
        assertTrue("Unexpected object type : " + factory, factory instanceof PooledConnectionFactory);
        assertEquals("Unexpected connection URL", TEST_CONNECTION_URL, ((PooledConnectionFactory)factory).getConnectionURL().getURL());
    }

    public void testGetObjectInstanceOfPooledConnectionFactoryWithMaxPoolSizeAndConnectionTimeout() throws Exception
    {
        Reference reference = createMockReference(PooledConnectionFactory.class.getName(),
                JNDI_ADDRESS_CONNECTION_URL, TEST_CONNECTION_URL);

        RefAddr maxPoolSizeRefAddr = mock(RefAddr.class);
        when(reference.get(JNDI_ADDRESS_MAX_POOL_SIZE)).thenReturn(maxPoolSizeRefAddr);
        when(maxPoolSizeRefAddr.getContent()).thenReturn(20);

        RefAddr connectionTimeoutRefAddr = mock(RefAddr.class);
        when(reference.get(JNDI_ADDRESS_CONNECTION_TIMEOUT)).thenReturn(connectionTimeoutRefAddr);
        when(connectionTimeoutRefAddr.getContent()).thenReturn(2000l);

        Object factory = _objectFactory.getObjectInstance(reference, _name, _context, _environment);
        assertTrue("Unexpected object type : " + factory, factory instanceof PooledConnectionFactory);
        PooledConnectionFactory pooledConnectionFactory = (PooledConnectionFactory) factory;
        assertEquals("Unexpected max pool size", 20, pooledConnectionFactory.getMaxPoolSize());
        assertEquals("Unexpected timeout", 2000l, pooledConnectionFactory.getConnectionTimeout());
    }

    private Reference createMockReference(String className, String addressType, Object content)
    {
        Reference reference = mock(Reference.class);
        when(reference.getClassName()).thenReturn(className);
        RefAddr mockedRefAddr = mock(RefAddr.class);
        when(reference.get(addressType)).thenReturn(mockedRefAddr);
        when(mockedRefAddr.getContent()).thenReturn(content);
        return reference;
    }

    private boolean equals(Address address1, Address address2)
    {
        if (address1 == address2)
        {
            return true;
        }

        if ((address1 == null && address2 != null) || (address1 != null && address2 == null))
        {
            return false ;
        }

        if (address1.getClass() != address2.getClass())
        {
            return false;
        }

        if (address1.getName() != null ? !address1.getName().equals(address2.getName()) : address2.getName() != null)
        {
            return false;
        }

        if (address1.getSubject() != null ? !address1.getSubject().equals(address2.getSubject()) : address2.getSubject() != null)
        {
            return false;
        }

        return !(address1.getOptions() != null ? !address1.getOptions().equals(address2.getOptions()) : address2.getOptions() != null);
    }
}
