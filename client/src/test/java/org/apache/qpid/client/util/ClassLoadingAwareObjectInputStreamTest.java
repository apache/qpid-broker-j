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
package org.apache.qpid.client.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.qpid.client.util.ClassLoadingAwareObjectInputStream.TrustedClassFilter;
import org.apache.qpid.test.utils.QpidTestCase;

public class ClassLoadingAwareObjectInputStreamTest extends QpidTestCase
{
    private final TrustedClassFilter ACCEPTS_ALL_FILTER = new TrustedClassFilter()
    {

        @Override
        public boolean isTrusted(Class<?> clazz)
        {
            return true;
        }
    };

    private final TrustedClassFilter ACCEPTS_NONE_FILTER = new TrustedClassFilter()
    {

        @Override
        public boolean isTrusted(Class<?> clazz)
        {
            return false;
        }
    };

    private InputStream _in;
    private ClassLoadingAwareObjectInputStream _claOIS;

    protected void setUp() throws Exception
    {
        super.setUp();
        //Create a viable input stream for instantiating the CLA OIS
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        ObjectOutputStream out = new ObjectOutputStream(baos);
        out.writeObject("testString");
        out.flush();
        out.close();

        _in = new ByteArrayInputStream(baos.toByteArray());

        _claOIS = new ClassLoadingAwareObjectInputStream(_in, null);
    }

    @Override
    public void tearDown() throws Exception
    {
        _claOIS.close();
        super.tearDown();
    }

    /**
     * Test that the resolveProxyClass method returns a proxy class implementing the desired interface
     */
    public void testResolveProxyClass() throws Exception
    {
        //try to proxy an interface
        Class<?> clazz = _claOIS.resolveProxyClass(new String[]{"java.lang.CharSequence"});

        //verify the proxy supports the expected interface (only)
        List<Class<?>> interfaces = Arrays.<Class<?>>asList(clazz.getInterfaces());
        assertTrue("Unexpected interfaces supported by proxy", interfaces.contains(CharSequence.class));
        assertEquals("Unexpected interfaces supported by proxy", 1, interfaces.size());
    }

    /**
     * Test that the resolveProxyClass method throws a ClassNotFoundException wrapping an
     * IllegalArgumentException if it is provided arguments which violate the restrictions allowed
     * by Proxy.getProxyClass (as required by the ObjectInputStream.resolveProxyClass javadoc).
     */
    public void testResolveProxyClassThrowsCNFEWrappingIAE() throws Exception
    {
        try
        {
            //try to proxy a *class* rather than an interface, which is illegal
            _claOIS.resolveProxyClass(new String[]{"java.lang.String"});
            fail("should have thrown an exception");
        }
        catch (ClassNotFoundException cnfe)
        {
            //expected, but must verify it is wrapping an IllegalArgumentException
            assertTrue(cnfe.getCause() instanceof IllegalArgumentException);
        }
    }


    public void testReadObject() throws Exception
    {
        // Expect to succeed
        doTestReadObject(new SimplePojo("testString"), ACCEPTS_ALL_FILTER);

        // Expect to fail
        try
        {
            doTestReadObject(new SimplePojo("testString"), ACCEPTS_NONE_FILTER);
            fail("Should have failed to read");
        }
        catch (ClassNotFoundException cnfe)
        {
            // Expected
        }
    }

    public void testReadObjectByte() throws Exception
    {
        doTestReadObject(Byte.valueOf((byte) 255), ACCEPTS_ALL_FILTER);
    }

    public void testReadObjectShort() throws Exception
    {
        doTestReadObject(Short.valueOf((short) 255), ACCEPTS_ALL_FILTER);
    }

    public void testReadObjectInteger() throws Exception
    {
        doTestReadObject(Integer.valueOf(255), ACCEPTS_ALL_FILTER);
    }

    public void testReadObjectLong() throws Exception
    {
        doTestReadObject(Long.valueOf(255l), ACCEPTS_ALL_FILTER);
    }

    public void testReadObjectFloat() throws Exception
    {
        doTestReadObject(Float.valueOf(255.0f), ACCEPTS_ALL_FILTER);
    }

    public void testReadObjectDouble() throws Exception
    {
        doTestReadObject(Double.valueOf(255.0), ACCEPTS_ALL_FILTER);
    }

    public void testReadObjectBoolean() throws Exception
    {
        doTestReadObject(Boolean.FALSE, ACCEPTS_ALL_FILTER);
    }

    public void testReadObjectString() throws Exception
    {
        doTestReadObject(new String("testString"), ACCEPTS_ALL_FILTER);
    }

    public void testReadObjectIntArray() throws Exception
    {
        doTestReadObject(new int[]{1, 2, 3}, ACCEPTS_ALL_FILTER);
    }

    public void testPrimitiveByteNotFiltered() throws Exception
    {
        doTestReadPrimitive((byte) 255, ACCEPTS_NONE_FILTER);
    }

    public void testPrimitiveShortNotFiltered() throws Exception
    {
        doTestReadPrimitive((short) 255, ACCEPTS_NONE_FILTER);
    }

    public void testPrimitiveIntegerNotFiltered() throws Exception
    {
        doTestReadPrimitive((int) 255, ACCEPTS_NONE_FILTER);
    }

    public void testPrimitiveLongNotFiltered() throws Exception
    {
        doTestReadPrimitive((long) 255, ACCEPTS_NONE_FILTER);
    }

    public void testPrimitiveFloatNotFiltered() throws Exception
    {
        doTestReadPrimitive((float) 255.0, ACCEPTS_NONE_FILTER);
    }

    public void testPrimitiveDoubleNotFiltered() throws Exception
    {
        doTestReadPrimitive((double) 255.0, ACCEPTS_NONE_FILTER);
    }

    public void testPrimitiveBooleanNotFiltered() throws Exception
    {
        doTestReadPrimitive(false, ACCEPTS_NONE_FILTER);
    }

    public void testPrimitveCharNotFiltered() throws Exception
    {
        doTestReadPrimitive('c', ACCEPTS_NONE_FILTER);
    }

    public void testReadObjectStringNotFiltered() throws Exception
    {
        doTestReadObject(new String("testString"), ACCEPTS_NONE_FILTER);
    }

    public void testReadObjectFailsWithUntrustedType() throws Exception
    {
        byte[] serialized = serializeObject(new SimplePojo("testPayload"));

        TrustedClassFilter myFilter = new TrustedClassFilter()
        {

            @Override
            public boolean isTrusted(Class<?> clazz)
            {
                return !clazz.equals(SimplePojo.class);
            }
        };

        ByteArrayInputStream input = new ByteArrayInputStream(serialized);
        try (ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, myFilter))
        {
            try
            {
                reader.readObject();
                fail("Should not be able to read the payload.");
            }
            catch (ClassNotFoundException ex)
            {
            }
        }

        serialized = serializeObject(UUID.randomUUID());
        input = new ByteArrayInputStream(serialized);
        try (ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, myFilter))
        {
            try
            {
                reader.readObject();
            }
            catch (ClassNotFoundException ex)
            {
                fail("Should be able to read the payload.");
            }
        }
    }

    public void testReadObjectFailsWithUntrustedContentInTrustedType() throws Exception
    {
        byte[] serialized = serializeObject(new SimplePojo(UUID.randomUUID()));

        TrustedClassFilter myFilter = new TrustedClassFilter()
        {

            @Override
            public boolean isTrusted(Class<?> clazz)
            {
                return clazz.equals(SimplePojo.class);
            }
        };

        ByteArrayInputStream input = new ByteArrayInputStream(serialized);
        try (ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, myFilter))
        {
            try
            {
                reader.readObject();
                fail("Should not be able to read the payload.");
            }
            catch (ClassNotFoundException ex)
            {
            }
        }

        serialized = serializeObject(UUID.randomUUID());
        input = new ByteArrayInputStream(serialized);
        try (ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, myFilter))
        {
            try
            {
                reader.readObject();
                fail("Should not be able to read the payload.");
            }
            catch (ClassNotFoundException ex)
            {
            }
        }
    }

    private void doTestReadObject(Object value, TrustedClassFilter filter) throws Exception
    {
        byte[] serialized = serializeObject(value);

        ByteArrayInputStream input = new ByteArrayInputStream(serialized);
        try (ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, filter))
        {
            Object result = reader.readObject();
            assertNotNull(result);
            assertEquals(value.getClass(), result.getClass());
            if (value.getClass().isArray())
            {
                assertEquals(Array.getLength(value), Array.getLength(result));
                for (int i = 0; i < Array.getLength(value); ++i)
                {
                    assertEquals(Array.get(value, i), Array.get(result, i));
                }
            }
            else
            {
                assertEquals(value, result);
            }
        }
    }

    private byte[] serializeObject(Object value) throws IOException
    {
        byte[] result = new byte[0];

        if (value != null)
        {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(baos))
            {

                oos.writeObject(value);
                oos.flush();
                oos.close();

                result = baos.toByteArray();
            }
        }

        return result;
    }


    private void doTestReadPrimitive(Object value, TrustedClassFilter filter) throws Exception
    {
        byte[] serialized = serializePrimitive(value);

        ByteArrayInputStream input = new ByteArrayInputStream(serialized);
        try (ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, filter))
        {
            Object result = null;

            if (value instanceof Byte)
            {
                result = reader.readByte();
            }
            else if (value instanceof Short)
            {
                result = reader.readShort();
            }
            else if (value instanceof Integer)
            {
                result = reader.readInt();
            }
            else if (value instanceof Long)
            {
                result = reader.readLong();
            }
            else if (value instanceof Float)
            {
                result = reader.readFloat();
            }
            else if (value instanceof Double)
            {
                result = reader.readDouble();
            }
            else if (value instanceof Boolean)
            {
                result = reader.readBoolean();
            }
            else if (value instanceof Character)
            {
                result = reader.readChar();
            }
            else
            {
                throw new IllegalArgumentException("unsuitable type for primitive deserialization");
            }

            assertNotNull(result);
            assertEquals(value.getClass(), result.getClass());
            assertEquals(value, result);
        }
    }

    private byte[] serializePrimitive(Object value) throws IOException
    {
        byte[] result = new byte[0];

        if (value != null)
        {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(baos))
            {

                if (value instanceof Byte)
                {
                    oos.writeByte((byte) value);
                }
                else if (value instanceof Short)
                {
                    oos.writeShort((short) value);
                }
                else if (value instanceof Integer)
                {
                    oos.writeInt((int) value);
                }
                else if (value instanceof Long)
                {
                    oos.writeLong((long) value);
                }
                else if (value instanceof Float)
                {
                    oos.writeFloat((float) value);
                }
                else if (value instanceof Double)
                {
                    oos.writeDouble((double) value);
                }
                else if (value instanceof Boolean)
                {
                    oos.writeBoolean((boolean) value);
                }
                else if (value instanceof Character)
                {
                    oos.writeChar((char) value);
                }
                else
                {
                    throw new IllegalArgumentException("unsuitable type for primitive serialization");
                }

                oos.flush();
                oos.close();

                result = baos.toByteArray();
            }
        }

        return result;
    }
}
