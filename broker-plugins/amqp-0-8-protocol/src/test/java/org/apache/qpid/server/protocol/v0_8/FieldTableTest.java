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
package org.apache.qpid.server.protocol.v0_8;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.test.utils.UnitTestBase;

public class FieldTableTest extends UnitTestBase
{

    /**
     * Set a boolean and check that we can only get it back as a boolean and a string
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testBoolean()
    {
        FieldTable table1 = FieldTableFactory.createFieldTable(Collections.singletonMap("value", true));
        assertTrue(table1.containsKey("value"));
        assertEquals(true, table1.get("value"));
    }

    /**
     * Set a byte and check that we can only get it back as a byte and a string
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testByte()
    {
        FieldTable table1 = FieldTableFactory.createFieldTable(Collections.singletonMap("value", Byte.MAX_VALUE));
        assertTrue(table1.containsKey("value"));
        assertEquals(Byte.MAX_VALUE, table1.get("value"));
    }

    /**
     * Set a short and check that we can only get it back as a short and a string
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testShort()
    {
        FieldTable table1 = FieldTableFactory.createFieldTable(Collections.singletonMap("value", Short.MAX_VALUE));
        assertTrue(table1.containsKey("value"));
        assertEquals(Short.MAX_VALUE, table1.get("value"));
    }

    /**
     * Set a char and check that we can only get it back as a char
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testChar()
    {
        FieldTable table1 = FieldTableFactory.createFieldTable(Collections.singletonMap("value", 'c'));
        assertTrue(table1.containsKey("value"));
        assertEquals('c', table1.get("value"));
    }

    /**
     * Set a double and check that we can only get it back as a double
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testDouble()
    {
        FieldTable table1 = FieldTableFactory.createFieldTable(Collections.singletonMap("value", Double.MAX_VALUE));
        assertTrue(table1.containsKey("value"));
        assertEquals(Double.MAX_VALUE, table1.get("value"));
    }

    /**
     * Set a float and check that we can only get it back as a float
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testFloat()
    {
        FieldTable table1 = FieldTableFactory.createFieldTable(Collections.singletonMap("value", Float.MAX_VALUE));
        assertTrue(table1.containsKey("value"));
        assertEquals(Float.MAX_VALUE, table1.get("value"));
    }

    /**
     * Set an int and check that we can only get it back as an int
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testInt()
    {
        FieldTable table1 = FieldTableFactory.createFieldTable(Collections.singletonMap("value", Integer.MAX_VALUE));
        assertTrue(table1.containsKey("value"));
        assertEquals(Integer.MAX_VALUE, table1.get("value"));
    }

    /**
     * Set a long and check that we can only get it back as a long
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testLong()
    {
        FieldTable table1 = FieldTableFactory.createFieldTable(Collections.singletonMap("value", Long.MAX_VALUE));
        assertTrue(table1.containsKey("value"));
        assertEquals(Long.MAX_VALUE, table1.get("value"));
    }

    /**
     * Set a double and check that we can only get it back as a double
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testBytes()
    {
        byte[] bytes = { 99, 98, 97, 96, 95 };

        FieldTable table1 = FieldTableFactory.createFieldTable(Collections.singletonMap("value", bytes));
        assertTrue(table1.containsKey("value"));
        assertBytesEqual(bytes, (byte[])table1.get("value"));
    }

    /**
     * Set a String and check that we can only get it back as a String
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testString()
    {
        FieldTable table1 = FieldTableFactory.createFieldTable(Collections.singletonMap("value", "Hello"));
        assertTrue(table1.containsKey("value"));

        assertEquals("Hello", table1.get("value"));
    }

    /** Check that a nested field table parameter correctly encodes and decodes to a byte buffer. */
    @Test
    public void testNestedFieldTable() throws AMQFrameDecodingException
    {
        byte[] testBytes = new byte[] { 0, 1, 2, 3, 4, 5 };

        Map<String, Object> innerTable = new LinkedHashMap<>();

        // Put some stuff in the inner table.
        innerTable.put("bool", true);
        innerTable.put("byte", Byte.MAX_VALUE);
        innerTable.put("bytes", testBytes);
        innerTable.put("char", 'c');
        innerTable.put("double", Double.MAX_VALUE);
        innerTable.put("float", Float.MAX_VALUE);
        innerTable.put("int", Integer.MAX_VALUE);
        innerTable.put("long", Long.MAX_VALUE);
        innerTable.put("short", Short.MAX_VALUE);
        innerTable.put("string", "hello");
        innerTable.put("null-string", null);
        innerTable.put("field-array",Arrays.asList("hello", 42, Collections.emptyList()));

        // Put the inner table in the outer one.
        FieldTable outerTable = FieldTableFactory.createFieldTable(Collections.singletonMap("innerTable",
                                                                                         FieldTableFactory.createFieldTable(innerTable)));

        // Write the outer table into the buffer.
        QpidByteBuffer buf = QpidByteBuffer.allocate(EncodingUtils.encodedFieldTableLength(outerTable));

        outerTable.writeToBuffer(buf);

        buf.flip();

        // Extract the table back from the buffer again.
        FieldTable extractedOuterTable = EncodingUtils.readFieldTable(buf);
        assertNotNull("Unexpected outer table", extractedOuterTable);

        FieldTable extractedTable = (FieldTable)extractedOuterTable.get("innerTable");

        assertEquals(Boolean.TRUE, extractedTable.get("bool"));
        assertEquals(Byte.MAX_VALUE, extractedTable.get("byte"));
        assertBytesEqual(testBytes, (byte[])extractedTable.get("bytes"));
        assertEquals('c', extractedTable.get("char"));
        assertEquals(Double.MAX_VALUE, extractedTable.get("double"));
        assertEquals(Float.MAX_VALUE, extractedTable.get("float"));
        assertEquals(Integer.MAX_VALUE, extractedTable.get("int"));
        assertEquals(Long.MAX_VALUE, extractedTable.get("long"));
        assertEquals(Short.MAX_VALUE, extractedTable.get("short"));
        assertEquals("hello", extractedTable.get("string"));
        assertNull(extractedTable.get("null-string"));
        Collection fieldArray = (Collection) extractedTable.get("field-array");
        assertEquals(3, fieldArray.size());
        Iterator iter = fieldArray.iterator();
        assertEquals("hello", iter.next());
        assertEquals(42, iter.next());
        assertTrue(((Collection)iter.next()).isEmpty());
    }

    public void testUnsupportedObject()
    {
        try
        {
            FieldTableFactory.createFieldTable(Collections.singletonMap("value", new Exception()));
            fail("Non primitive values are not allowed");
        }
        catch (AMQPInvalidClassException aice)
        {
            assertEquals("Non primitive values are not allowed to be set",
                         AMQPInvalidClassException.INVALID_OBJECT_MSG + Exception.class,
                         aice.getMessage());
        }
    }

    @Test
    public void testValues() throws Exception
    {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("bool", true);
        map.put("byte", Byte.MAX_VALUE);
        byte[] bytes = { 99, 98, 97, 96, 95 };
        map.put("bytes", bytes);
        map.put("char", 'c');
        map.put("double", Double.MAX_VALUE);
        map.put("float", Float.MAX_VALUE);
        map.put("int", Integer.MAX_VALUE);
        map.put("long", Long.MAX_VALUE);
        map.put("short", Short.MAX_VALUE);
        map.put("string", "Hello");
        map.put("null-string", null);
        map.put("object-bool", true);
        map.put("object-byte", Byte.MAX_VALUE);
        map.put("object-bytes", bytes);
        map.put("object-char", 'c');
        map.put("object-double", Double.MAX_VALUE);
        map.put("object-float", Float.MAX_VALUE);
        map.put("object-int", Integer.MAX_VALUE);
        map.put("object-long", Long.MAX_VALUE);
        map.put("object-short", Short.MAX_VALUE);
        map.put("object-string", "Hello");

        FieldTable mapTable = FieldTableFactory.createFieldTable(map);
        QpidByteBuffer buf = QpidByteBuffer.allocate(EncodingUtils.encodedFieldTableLength(mapTable));
        mapTable.writeToBuffer(buf);
        buf.flip();

        FieldTable table = EncodingUtils.readFieldTable(buf);
        assertNotNull(table);

        assertEquals(Boolean.TRUE, table.get("bool"));
        assertEquals(Byte.MAX_VALUE, table.get("byte"));
        assertBytesEqual(bytes, (byte[])table.get("bytes"));
        assertEquals('c', table.get("char"));
        assertEquals(Double.MAX_VALUE, table.get("double"));
        assertEquals(Float.MAX_VALUE, table.get("float"));
        assertEquals(Integer.MAX_VALUE, table.get("int"));
        assertEquals(Long.MAX_VALUE, table.get("long"));
        assertEquals(Short.MAX_VALUE, table.get("short"));
        assertEquals("Hello", table.get("string"));
        assertNull(table.get("null-string"));
        assertEquals(true, table.get("object-bool"));
        assertEquals(Byte.MAX_VALUE, table.get("object-byte"));
        assertBytesEqual(bytes, (byte[]) table.get("object-bytes"));
        assertEquals('c', table.get("object-char"));
        assertEquals(Double.MAX_VALUE, table.get("object-double"));
        assertEquals(Float.MAX_VALUE, table.get("object-float"));
        assertEquals(Integer.MAX_VALUE, table.get("object-int"));
        assertEquals(Long.MAX_VALUE, table.get("object-long"));
        assertEquals(Short.MAX_VALUE, table.get("object-short"));
        assertEquals("Hello", table.get("object-string"));
    }

    @Test
    public void testWriteBuffer()
    {
        byte[] bytes = { 99, 98, 97, 96, 95 };

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("bool", true);
        map.put("byte", Byte.MAX_VALUE);
        map.put("bytes", bytes);
        map.put("char", 'c');
        map.put("int", Integer.MAX_VALUE);
        map.put("long", Long.MAX_VALUE);
        map.put("double", Double.MAX_VALUE);
        map.put("float", Float.MAX_VALUE);
        map.put("short", Short.MAX_VALUE);
        map.put("string", "hello");
        map.put("null-string", null);

        FieldTable table = FieldTableFactory.createFieldTable(map);

        QpidByteBuffer buf = QpidByteBuffer.allocate((int) table.getEncodedSize() + 4);
        table.writeToBuffer(buf);

        buf.flip();

        long length = buf.getInt() & 0xFFFFFFFFL;
        QpidByteBuffer bufSlice = buf.slice();
        bufSlice.limit((int)length);


        FieldTable table2 = FieldTableFactory.createFieldTable(buf);
        assertNotNull(table2);

        assertEquals(true, table2.get("bool"));
        assertEquals(Byte.MAX_VALUE, table2.get("byte"));
        assertBytesEqual(bytes, (byte[])table2.get("bytes"));
        assertEquals('c', table2.get("char"));
        assertEquals(Double.MAX_VALUE, table2.get("double"));
        assertEquals(Float.MAX_VALUE, table2.get("float"));
        assertEquals(Integer.MAX_VALUE, table2.get("int"));
        assertEquals(Long.MAX_VALUE, table2.get("long"));
        assertEquals(Short.MAX_VALUE, table2.get("short"));
        assertEquals("hello", table2.get("string"));
        assertNull(table2.get("null-string"));
        buf.dispose();
        bufSlice.dispose();
        table.dispose();
        table2.dispose();
    }

    @Test
    public void testEncodingSize()
    {
        Map<String, Object> result = new LinkedHashMap<>();
        int size = 0;

        result.put("boolean", true);
        size += 1 + EncodingUtils.encodedShortStringLength("boolean") + EncodingUtils.encodedBooleanLength();

        result.put("byte", Byte.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("byte") + EncodingUtils.encodedByteLength();

        byte[] _bytes = { 99, 98, 97, 96, 95 };

        result.put("bytes", _bytes);
        size += 1 + EncodingUtils.encodedShortStringLength("bytes") + 4 + _bytes.length;

        result.put("char", 'c');
        size += 1 + EncodingUtils.encodedShortStringLength("char") + EncodingUtils.encodedCharLength();

        result.put("double", Double.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("double") + EncodingUtils.encodedDoubleLength();

        result.put("float", Float.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("float") + EncodingUtils.encodedFloatLength();

        result.put("int", Integer.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("int") + EncodingUtils.encodedIntegerLength();

        result.put("long", Long.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("long") + EncodingUtils.encodedLongLength();

        result.put("short", Short.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("short") + EncodingUtils.encodedShortLength();

        result.put("result", "Hello");
        size += 1 + EncodingUtils.encodedShortStringLength("result") + EncodingUtils.encodedLongStringLength("Hello");

        result.put("object-bool", true);
        size += 1 + EncodingUtils.encodedShortStringLength("object-bool") + EncodingUtils.encodedBooleanLength();

        result.put("object-byte", Byte.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("object-byte") + EncodingUtils.encodedByteLength();

        result.put("object-bytes", _bytes);
        size += 1 + EncodingUtils.encodedShortStringLength("object-bytes") + 4 + _bytes.length;

        result.put("object-char", 'c');
        size += 1 + EncodingUtils.encodedShortStringLength("object-char") + EncodingUtils.encodedCharLength();

        result.put("object-double", Double.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("object-double") + EncodingUtils.encodedDoubleLength();

        result.put("object-float", Float.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("object-float") + EncodingUtils.encodedFloatLength();

        result.put("object-int", Integer.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("object-int") + EncodingUtils.encodedIntegerLength();

        result.put("object-long", Long.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("object-long") + EncodingUtils.encodedLongLength();

        result.put("object-short", Short.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("object-short") + EncodingUtils.encodedShortLength();

        assertEquals(size, FieldTableFactory.createFieldTable(result).getEncodedSize());
    }

    /**
     * Additional test checkPropertyName doesn't accept Null
     */
    @Test
    public void testCheckPropertyNameIsNull()
    {
        try
        {
            FieldTableFactory.createFieldTable(Collections.singletonMap(null, "String"));
            fail("Null property name is not allowed");
        }
        catch (IllegalArgumentException iae)
        {
            // normal path
        }
    }

    /**
     * Additional test checkPropertyName doesn't accept an empty String
     */
    @Test
    public void testCheckPropertyNameIsEmptyString()
    {
        try
        {
            FieldTableFactory.createFieldTable(Collections.singletonMap("", "String"));
            fail("empty property name is not allowed");
        }
        catch (IllegalArgumentException iae)
        {
            // normal path
        }
    }

    /**
     * Additional test checkPropertyName doesn't accept an empty String
     */
    @Test
    public void testCheckPropertyNameHasMaxLength()
    {
        StringBuilder longPropertyName = new StringBuilder(129);

        for (int i = 0; i < 129; i++)
        {
            longPropertyName.append("x");
        }

        boolean strictAMQP = FieldTable._strictAMQP;
        try
        {
            FieldTable._strictAMQP = true;
            new FieldTable(Collections.singletonMap(longPropertyName.toString(), "String"));
            fail("property name must be < 128 characters");
        }
        catch (IllegalArgumentException iae)
        {
            // normal path
        }
        finally
        {
            FieldTable._strictAMQP = strictAMQP;
        }
    }

    /**
     * Additional test checkPropertyName starts with a letter
     */
    @Test
    public void testCheckPropertyNameStartCharacterIsLetter()
    {
        boolean strictAMQP = FieldTable._strictAMQP;

        // Try a name that starts with a number
        try
        {
            FieldTable._strictAMQP = true;
            new FieldTable(Collections.singletonMap("1", "String"));
            fail("property name must start with a letter");
        }
        catch (IllegalArgumentException iae)
        {
            // normal path
        }
        finally
        {
            FieldTable._strictAMQP = strictAMQP;
        }
    }

    /**
     * Additional test checkPropertyName starts with a hash or a dollar
     */
    @Test
    public void testCheckPropertyNameStartCharacterIsHashOrDollar()
    {
        try
        {
            FieldTableFactory.createFieldTable(Collections.singletonMap("#", "String"));
        }
        catch (IllegalArgumentException iae)
        {
            fail("property name are allowed to start with # and $s");
        }

        try
        {
            FieldTableFactory.createFieldTable(Collections.singletonMap("$", "String"));
        }
        catch (IllegalArgumentException iae)
        {
            fail("property name are allowed to start with # and $s");
        }
    }

    @Test
    public void testValidateMalformedFieldTable()
    {
        final FieldTable fieldTable = buildMalformedFieldTable();

        try
        {
            fieldTable.validate();
            fail("Exception is expected");
        }
        catch (RuntimeException e)
        {
            // pass
        }
    }

    @Test
    public void testValidateCorrectFieldTable()
    {
        final FieldTable ft = new FieldTable(Collections.singletonMap("testKey", "testValue"));
        final int encodedSize = (int)ft.getEncodedSize() + Integer.BYTES;
        final QpidByteBuffer buf = QpidByteBuffer.allocate(encodedSize);
        ft.writeToBuffer(buf);
        buf.flip();
        buf.position(Integer.BYTES);

        final FieldTable fieldTable = new FieldTable(buf);
        assertEquals(1, fieldTable.size());
        fieldTable.validate();
        assertTrue("Expected key is not found", fieldTable.containsKey("testKey"));
    }

    private FieldTable buildMalformedFieldTable()
    {
        final QpidByteBuffer buf = QpidByteBuffer.allocate(1);
        buf.put((byte) -1);

        buf.flip();

        return FieldTableFactory.createFieldTable(buf);
    }

    private void assertBytesEqual(byte[] expected, byte[] actual)
    {
        assertEquals(expected.length, actual.length);

        for (int index = 0; index < expected.length; index++)
        {
            assertEquals(expected[index], actual[index]);
        }
    }

    public static junit.framework.Test suite()
    {
        return new junit.framework.TestSuite(FieldTableTest.class);
    }

}
