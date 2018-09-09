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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.test.utils.UnitTestBase;

public class FieldTableTest extends UnitTestBase
{
    /**
     * Test that setting a similar named value replaces any previous value set on that name
     */
    @Test
    public void testReplacement()
    {
        FieldTable table1 = new FieldTable();
        // Set a boolean value
        table1.setBoolean("value", true);
        // Check length of table is correct (<Value length> + <type> + <Boolean length>)
        int size = EncodingUtils.encodedShortStringLength("value") + 1 + EncodingUtils.encodedBooleanLength();
        assertEquals(size, table1.getEncodedSize());

        // reset value to an integer
        table1.setInteger("value", Integer.MAX_VALUE);

        // Check the length has changed accordingly   (<Value length> + <type> + <Integer length>)
        size = EncodingUtils.encodedShortStringLength("value") + 1 + EncodingUtils.encodedIntegerLength();
        assertEquals(size, table1.getEncodedSize());

        // Check boolean value is null
        assertEquals(null, table1.getBoolean("value"));
        // ... and integer value is good
        assertEquals((Integer) Integer.MAX_VALUE, table1.getInteger("value"));
    }

    /**
     * Set a boolean and check that we can only get it back as a boolean and a string
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testBoolean()
    {
        FieldTable table1 = new FieldTable();
        table1.setBoolean("value", true);
        assertTrue(table1.propertyExists("value"));

        // Test Getting right value back
        assertEquals((Boolean) true, table1.getBoolean("value"));

        // Check we don't get anything back for other gets
        assertEquals(null, table1.getByte("value"));
        assertEquals(null, table1.getByte("value"));
        assertEquals(null, table1.getShort("value"));
        assertEquals(null, table1.getCharacter("value"));
        assertEquals(null, table1.getDouble("value"));
        assertEquals(null, table1.getFloat("value"));
        assertEquals(null, table1.getInteger("value"));
        assertEquals(null, table1.getLong("value"));
        assertEquals(null, table1.getBytes("value"));

        // except value as a string
        assertEquals("true", table1.getString("value"));

        table1.remove("value");

        // Table should now have zero length for encoding
        checkEmpty(table1);

        // Looking up an invalid value returns null
        assertEquals(null, table1.getBoolean("Rubbish"));
    }

    /**
     * Set a byte and check that we can only get it back as a byte and a string
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testByte()
    {
        FieldTable table1 = new FieldTable();
        table1.setByte("value", Byte.MAX_VALUE);
        assertTrue(table1.propertyExists("value"));

        // Tests lookups we shouldn't get anything back for other gets
        // we should get right value back for this type ....
        assertEquals(null, table1.getBoolean("value"));
        assertEquals(Byte.valueOf(Byte.MAX_VALUE), table1.getByte("value"));
        assertEquals(null, table1.getShort("value"));
        assertEquals(null, table1.getCharacter("value"));
        assertEquals(null, table1.getDouble("value"));
        assertEquals(null, table1.getFloat("value"));
        assertEquals(null, table1.getInteger("value"));
        assertEquals(null, table1.getLong("value"));
        assertEquals(null, table1.getBytes("value"));

        // ... and a the string value of it.
        assertEquals("" + Byte.MAX_VALUE, table1.getString("value"));

        table1.remove("value");
        // Table should now have zero length for encoding
        checkEmpty(table1);

        // Looking up an invalid value returns null
        assertEquals(null, table1.getByte("Rubbish"));
    }

    /**
     * Set a short and check that we can only get it back as a short and a string
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testShort()
    {
        FieldTable table1 = new FieldTable();
        table1.setShort("value", Short.MAX_VALUE);
        assertTrue(table1.propertyExists("value"));

        // Tests lookups we shouldn't get anything back for other gets
        // we should get right value back for this type ....
        assertEquals(null, table1.getBoolean("value"));
        assertEquals(null, table1.getByte("value"));
        assertEquals(Short.valueOf(Short.MAX_VALUE), table1.getShort("value"));
        assertEquals(null, table1.getCharacter("value"));
        assertEquals(null, table1.getDouble("value"));
        assertEquals(null, table1.getFloat("value"));
        assertEquals(null, table1.getInteger("value"));
        assertEquals(null, table1.getLong("value"));
        assertEquals(null, table1.getBytes("value"));

        // ... and a the string value of it.
        assertEquals("" + Short.MAX_VALUE, table1.getString("value"));

        table1.remove("value");
        // Table should now have zero length for encoding
        checkEmpty(table1);

        // Looking up an invalid value returns null
        assertEquals(null, table1.getShort("Rubbish"));
    }

    /**
     * Set a char and check that we can only get it back as a char
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testChar()
    {
        FieldTable table1 = new FieldTable();
        table1.setChar("value", 'c');
        assertTrue(table1.propertyExists("value"));

        // Tests lookups we shouldn't get anything back for other gets
        // we should get right value back for this type ....
        assertEquals(null, table1.getBoolean("value"));
        assertEquals(null, table1.getByte("value"));
        assertEquals(null, table1.getShort("value"));
        assertEquals(Character.valueOf('c'), table1.getCharacter("value"));
        assertEquals(null, table1.getDouble("value"));
        assertEquals(null, table1.getFloat("value"));
        assertEquals(null, table1.getInteger("value"));
        assertEquals(null, table1.getLong("value"));
        assertEquals(null, table1.getBytes("value"));

        // ... and a the string value of it.
        assertEquals("c", table1.getString("value"));

        table1.remove("value");

        // Table should now have zero length for encoding
        checkEmpty(table1);

        // Looking up an invalid value returns null
        assertEquals(null, table1.getCharacter("Rubbish"));
    }

    /**
     * Set a double and check that we can only get it back as a double
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testDouble()
    {
        FieldTable table1 = new FieldTable();
        table1.setDouble("value", Double.MAX_VALUE);
        assertTrue(table1.propertyExists("value"));

        // Tests lookups we shouldn't get anything back for other gets
        // we should get right value back for this type ....
        assertEquals(null, table1.getBoolean("value"));
        assertEquals(null, table1.getByte("value"));
        assertEquals(null, table1.getShort("value"));
        assertEquals(null, table1.getCharacter("value"));
        assertEquals(Double.valueOf(Double.MAX_VALUE), table1.getDouble("value"));
        assertEquals(null, table1.getFloat("value"));
        assertEquals(null, table1.getInteger("value"));
        assertEquals(null, table1.getLong("value"));
        assertEquals(null, table1.getBytes("value"));

        // ... and a the string value of it.
        assertEquals("" + Double.MAX_VALUE, table1.getString("value"));
        table1.remove("value");
        // but after a removeKey it doesn't
        assertFalse(table1.containsKey("value"));

        // Table should now have zero length for encoding
        checkEmpty(table1);

        // Looking up an invalid value returns null
        assertEquals(null, table1.getDouble("Rubbish"));
    }

    /**
     * Set a float and check that we can only get it back as a float
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testFloat()
    {
        FieldTable table1 = new FieldTable();
        table1.setFloat("value", Float.MAX_VALUE);
        assertTrue(table1.propertyExists("value"));

        // Tests lookups we shouldn't get anything back for other gets
        // we should get right value back for this type ....
        assertEquals(null, table1.getBoolean("value"));
        assertEquals(null, table1.getByte("value"));
        assertEquals(null, table1.getShort("value"));
        assertEquals(null, table1.getCharacter("value"));
        assertEquals(null, table1.getDouble("value"));
        assertEquals(Float.valueOf(Float.MAX_VALUE), table1.getFloat("value"));
        assertEquals(null, table1.getInteger("value"));
        assertEquals(null, table1.getLong("value"));
        assertEquals(null, table1.getBytes("value"));

        // ... and a the string value of it.
        assertEquals("" + Float.MAX_VALUE, table1.getString("value"));

        table1.remove("value");
        // but after a removeKey it doesn't
        assertFalse(table1.containsKey("value"));

        // Table should now have zero length for encoding
        checkEmpty(table1);

        // Looking up an invalid value returns null
        assertEquals(null, table1.getFloat("Rubbish"));
    }

    /**
     * Set an int and check that we can only get it back as an int
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testInt()
    {
        FieldTable table1 = new FieldTable();
        table1.setInteger("value", Integer.MAX_VALUE);
        assertTrue(table1.propertyExists("value"));

        // Tets lookups we shouldn't get anything back for other gets
        // we should get right value back for this type ....
        assertEquals(null, table1.getBoolean("value"));
        assertEquals(null, table1.getByte("value"));
        assertEquals(null, table1.getShort("value"));
        assertEquals(null, table1.getCharacter("value"));
        assertEquals(null, table1.getDouble("value"));
        assertEquals(null, table1.getFloat("value"));
        assertEquals(Integer.valueOf(Integer.MAX_VALUE), table1.getInteger("value"));
        assertEquals(null, table1.getLong("value"));
        assertEquals(null, table1.getBytes("value"));

        // ... and a the string value of it.
        assertEquals("" + Integer.MAX_VALUE, table1.getString("value"));

        table1.remove("value");
        // but after a removeKey it doesn't
        assertFalse(table1.containsKey("value"));

        // Table should now have zero length for encoding
        checkEmpty(table1);

        // Looking up an invalid value returns null
        assertEquals(null, table1.getInteger("Rubbish"));
    }

    /**
     * Set a long and check that we can only get it back as a long
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testLong()
    {
        FieldTable table1 = new FieldTable();
        table1.setLong("value", Long.MAX_VALUE);
        assertTrue(table1.propertyExists("value"));

        // Tets lookups we shouldn't get anything back for other gets
        // we should get right value back for this type ....
        assertEquals(null, table1.getBoolean("value"));
        assertEquals(null, table1.getByte("value"));
        assertEquals(null, table1.getShort("value"));
        assertEquals(null, table1.getCharacter("value"));
        assertEquals(null, table1.getDouble("value"));
        assertEquals(null, table1.getFloat("value"));
        assertEquals(null, table1.getInteger("value"));
        assertEquals(Long.valueOf(Long.MAX_VALUE), table1.getLong("value"));
        assertEquals(null, table1.getBytes("value"));

        // ... and a the string value of it.
        assertEquals("" + Long.MAX_VALUE, table1.getString("value"));

        table1.remove("value");
        // but after a removeKey it doesn't
        assertFalse(table1.containsKey("value"));

        // Table should now have zero length for encoding
        checkEmpty(table1);

        // Looking up an invalid value returns null
        assertEquals(null, table1.getLong("Rubbish"));
    }

    /**
     * Set a double and check that we can only get it back as a double
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testBytes()
    {
        byte[] bytes = { 99, 98, 97, 96, 95 };

        FieldTable table1 = new FieldTable();
        table1.setBytes("value", bytes);
        assertTrue(table1.propertyExists("value"));

        // Tets lookups we shouldn't get anything back for other gets
        // we should get right value back for this type ....
        assertEquals(null, table1.getBoolean("value"));
        assertEquals(null, table1.getByte("value"));
        assertEquals(null, table1.getShort("value"));
        assertEquals(null, table1.getCharacter("value"));
        assertEquals(null, table1.getDouble("value"));
        assertEquals(null, table1.getFloat("value"));
        assertEquals(null, table1.getInteger("value"));
        assertEquals(null, table1.getLong("value"));
        assertBytesEqual(bytes, table1.getBytes("value"));

        // ... and a the string value of it is null
        assertEquals(null, table1.getString("value"));

        table1.remove("value");
        // but after a removeKey it doesn't
        assertFalse(table1.containsKey("value"));

        // Table should now have zero length for encoding
        checkEmpty(table1);

        // Looking up an invalid value returns null
        assertEquals(null, table1.getBytes("Rubbish"));
    }

    /**
     * Calls all methods that can be used to check the table is empty
     * - getEncodedSize
     * - isEmpty
     * - length
     *
     * @param table to check is empty
     */
    private void checkEmpty(FieldTable table)
    {
        assertEquals(0, table.getEncodedSize());
        assertTrue(table.isEmpty());
        assertEquals(0, table.size());
    }

    /**
     * Set a String and check that we can only get it back as a String
     * Check that attempting to lookup a non existent value returns null
     */
    @Test
    public void testString()
    {
        FieldTable table1 = new FieldTable();
        table1.setString("value", "Hello");
        assertTrue(table1.propertyExists("value"));

        // Test lookups we shouldn't get anything back for other gets
        // we should get right value back for this type ....
        assertEquals(null, table1.getBoolean("value"));
        assertEquals(null, table1.getByte("value"));
        assertEquals(null, table1.getShort("value"));
        assertEquals(null, table1.getCharacter("value"));
        assertEquals(null, table1.getDouble("value"));
        assertEquals(null, table1.getFloat("value"));
        assertEquals(null, table1.getInteger("value"));
        assertEquals(null, table1.getLong("value"));
        assertEquals(null, table1.getBytes("value"));
        assertEquals("Hello", table1.getString("value"));

        // Try setting a null value and read it back
        table1.setString("value", null);

        assertEquals(null, table1.getString("value"));

        // but still contains the value
        assertTrue(table1.containsKey("value"));

        table1.remove("value");
        // but after a removeKey it doesn't
        assertFalse(table1.containsKey("value"));

        checkEmpty(table1);

        // Looking up an invalid value returns null
        assertEquals(null, table1.getString("Rubbish"));

        // Additional Test that haven't been covered for string
        table1.setObject("value", "Hello");
        // Check that it was set correctly
        assertEquals("Hello", table1.getString("value"));
    }

    /** Check that a nested field table parameter correctly encodes and decodes to a byte buffer. */
    @Test
    public void testNestedFieldTable() throws IOException
    {
        byte[] testBytes = new byte[] { 0, 1, 2, 3, 4, 5 };

        FieldTable outerTable = new FieldTable();
        FieldTable innerTable = new FieldTable();

        // Put some stuff in the inner table.
        innerTable.setBoolean("bool", true);
        innerTable.setByte("byte", Byte.MAX_VALUE);
        innerTable.setBytes("bytes", testBytes);
        innerTable.setChar("char", 'c');
        innerTable.setDouble("double", Double.MAX_VALUE);
        innerTable.setFloat("float", Float.MAX_VALUE);
        innerTable.setInteger("int", Integer.MAX_VALUE);
        innerTable.setLong("long", Long.MAX_VALUE);
        innerTable.setShort("short", Short.MAX_VALUE);
        innerTable.setString("string", "hello");
        innerTable.setString("null-string", null);
        innerTable.setFieldArray("field-array",Arrays.asList("hello",Integer.valueOf(42), Collections.emptyList()));

        // Put the inner table in the outer one.
        outerTable.setFieldTable("innerTable", innerTable);

        // Write the outer table into the buffer.
        QpidByteBuffer buf = QpidByteBuffer.allocate(EncodingUtils.encodedFieldTableLength(outerTable));

        outerTable.writeToBuffer(buf);

        buf.flip();

        // Extract the table back from the buffer again.
        try
        {
            FieldTable extractedOuterTable = EncodingUtils.readFieldTable(buf);

            FieldTable extractedTable = extractedOuterTable.getFieldTable("innerTable");

            assertEquals(Boolean.TRUE, extractedTable.getBoolean("bool"));
            assertEquals(Byte.valueOf(Byte.MAX_VALUE), extractedTable.getByte("byte"));
            assertBytesEqual(testBytes, extractedTable.getBytes("bytes"));
            assertEquals(Character.valueOf('c'), extractedTable.getCharacter("char"));
            assertEquals(Double.valueOf(Double.MAX_VALUE), extractedTable.getDouble("double"));
            assertEquals(Float.valueOf(Float.MAX_VALUE), extractedTable.getFloat("float"));
            assertEquals(Integer.valueOf(Integer.MAX_VALUE), extractedTable.getInteger("int"));
            assertEquals(Long.valueOf(Long.MAX_VALUE), extractedTable.getLong("long"));
            assertEquals(Short.valueOf(Short.MAX_VALUE), extractedTable.getShort("short"));
            assertEquals("hello", extractedTable.getString("string"));
            assertNull(extractedTable.getString("null-string"));
            Collection fieldArray = (Collection) extractedTable.get("field-array");
            assertEquals(3, fieldArray.size());
            Iterator iter = fieldArray.iterator();
            assertEquals("hello", iter.next());
            assertEquals(Integer.valueOf(42), iter.next());
            assertTrue(((Collection)iter.next()).isEmpty());
        }
        catch (AMQFrameDecodingException e)
        {
            fail("Failed to decode field table with nested inner table.");
        }
    }

    @Test
    public void testValues()
    {
        FieldTable table = new FieldTable();
        table.setBoolean("bool", true);
        table.setByte("byte", Byte.MAX_VALUE);
        byte[] bytes = { 99, 98, 97, 96, 95 };
        table.setBytes("bytes", bytes);
        table.setChar("char", 'c');
        table.setDouble("double", Double.MAX_VALUE);
        table.setFloat("float", Float.MAX_VALUE);
        table.setInteger("int", Integer.MAX_VALUE);
        table.setLong("long", Long.MAX_VALUE);
        table.setShort("short", Short.MAX_VALUE);
        table.setString("string", "Hello");
        table.setString("null-string", null);

        table.setObject("object-bool", true);
        table.setObject("object-byte", Byte.MAX_VALUE);
        table.setObject("object-bytes", bytes);
        table.setObject("object-char", 'c');
        table.setObject("object-double", Double.MAX_VALUE);
        table.setObject("object-float", Float.MAX_VALUE);
        table.setObject("object-int", Integer.MAX_VALUE);
        table.setObject("object-long", Long.MAX_VALUE);
        table.setObject("object-short", Short.MAX_VALUE);
        table.setObject("object-string", "Hello");

        try
        {
            table.setObject("Null-object", null);
            fail("null values are not allowed");
        }
        catch (AMQPInvalidClassException aice)
        {
            assertEquals("Null values are not allowed to be set",
                                AMQPInvalidClassException.INVALID_OBJECT_MSG + "null",
                                aice.getMessage());

        }

        try
        {
            table.setObject("Unsupported-object", new Exception());
            fail("Non primitive values are not allowed");
        }
        catch (AMQPInvalidClassException aice)
        {
            assertEquals("Non primitive values are not allowed to be set",
                                AMQPInvalidClassException.INVALID_OBJECT_MSG + Exception.class,
                                aice.getMessage());
        }

        assertEquals(Boolean.TRUE, table.getBoolean("bool"));
        assertEquals(Byte.valueOf(Byte.MAX_VALUE), table.getByte("byte"));
        assertBytesEqual(bytes, table.getBytes("bytes"));
        assertEquals(Character.valueOf('c'), table.getCharacter("char"));
        assertEquals(Double.valueOf(Double.MAX_VALUE), table.getDouble("double"));
        assertEquals(Float.valueOf(Float.MAX_VALUE), table.getFloat("float"));
        assertEquals(Integer.valueOf(Integer.MAX_VALUE), table.getInteger("int"));
        assertEquals(Long.valueOf(Long.MAX_VALUE), table.getLong("long"));
        assertEquals(Short.valueOf(Short.MAX_VALUE), table.getShort("short"));
        assertEquals("Hello", table.getString("string"));
        assertNull(table.getString("null-string"));

        assertEquals(true, table.getObject("object-bool"));
        assertEquals(Byte.MAX_VALUE, table.getObject("object-byte"));
        assertBytesEqual(bytes, (byte[]) table.getObject("object-bytes"));
        assertEquals('c', table.getObject("object-char"));
        assertEquals(Double.MAX_VALUE, table.getObject("object-double"));
        assertEquals(Float.MAX_VALUE, table.getObject("object-float"));
        assertEquals(Integer.MAX_VALUE, table.getObject("object-int"));
        assertEquals(Long.MAX_VALUE, table.getObject("object-long"));
        assertEquals(Short.MAX_VALUE, table.getObject("object-short"));
        assertEquals("Hello", table.getObject("object-string"));
    }

    @Test
    public void testWriteBuffer() throws IOException
    {
        byte[] bytes = { 99, 98, 97, 96, 95 };

        FieldTable table = new FieldTable();
        table.setBoolean("bool", true);
        table.setByte("byte", Byte.MAX_VALUE);

        table.setBytes("bytes", bytes);
        table.setChar("char", 'c');
        table.setInteger("int", Integer.MAX_VALUE);
        table.setLong("long", Long.MAX_VALUE);
        table.setDouble("double", Double.MAX_VALUE);
        table.setFloat("float", Float.MAX_VALUE);
        table.setShort("short", Short.MAX_VALUE);
        table.setString("string", "hello");
        table.setString("null-string", null);


        QpidByteBuffer buf = QpidByteBuffer.allocate((int) table.getEncodedSize() + 4);
        table.writeToBuffer(buf);

        buf.flip();

        long length = buf.getInt() & 0xFFFFFFFFL;
        QpidByteBuffer bufSlice = buf.slice();
        bufSlice.limit((int)length);


        FieldTable table2 = new FieldTable(bufSlice);

        assertEquals((Boolean) true, table2.getBoolean("bool"));
        assertEquals((Byte) Byte.MAX_VALUE, table2.getByte("byte"));
        assertBytesEqual(bytes, table2.getBytes("bytes"));
        assertEquals((Character) 'c', table2.getCharacter("char"));
        assertEquals(Double.valueOf(Double.MAX_VALUE), table2.getDouble("double"));
        assertEquals(Float.valueOf(Float.MAX_VALUE), table2.getFloat("float"));
        assertEquals(Integer.valueOf(Integer.MAX_VALUE), table2.getInteger("int"));
        assertEquals(Long.valueOf(Long.MAX_VALUE), table2.getLong("long"));
        assertEquals(Short.valueOf(Short.MAX_VALUE), table2.getShort("short"));
        assertEquals("hello", table2.getString("string"));
        assertNull(table2.getString("null-string"));
        buf.dispose();
        bufSlice.dispose();
        table.dispose();
        table2.dispose();
    }

    @Test
    public void testEncodingSize()
    {
        FieldTable result = new FieldTable();
        int size = 0;

        result.setBoolean("boolean", true);
        size += 1 + EncodingUtils.encodedShortStringLength("boolean") + EncodingUtils.encodedBooleanLength();
        assertEquals(size, result.getEncodedSize());

        result.setByte("byte", (byte) Byte.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("byte") + EncodingUtils.encodedByteLength();
        assertEquals(size, result.getEncodedSize());

        byte[] _bytes = { 99, 98, 97, 96, 95 };

        result.setBytes("bytes", _bytes);
        size += 1 + EncodingUtils.encodedShortStringLength("bytes") + 4 + _bytes.length;
        assertEquals(size, result.getEncodedSize());

        result.setChar("char", (char) 'c');
        size += 1 + EncodingUtils.encodedShortStringLength("char") + EncodingUtils.encodedCharLength();
        assertEquals(size, result.getEncodedSize());

        result.setDouble("double", (double) Double.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("double") + EncodingUtils.encodedDoubleLength();
        assertEquals(size, result.getEncodedSize());

        result.setFloat("float", (float) Float.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("float") + EncodingUtils.encodedFloatLength();
        assertEquals(size, result.getEncodedSize());

        result.setInteger("int", (int) Integer.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("int") + EncodingUtils.encodedIntegerLength();
        assertEquals(size, result.getEncodedSize());

        result.setLong("long", (long) Long.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("long") + EncodingUtils.encodedLongLength();
        assertEquals(size, result.getEncodedSize());

        result.setShort("short", (short) Short.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("short") + EncodingUtils.encodedShortLength();
        assertEquals(size, result.getEncodedSize());

        result.setString("result", "Hello");
        size += 1 + EncodingUtils.encodedShortStringLength("result") + EncodingUtils.encodedLongStringLength("Hello");
        assertEquals(size, result.getEncodedSize());

        result.setObject("object-bool", true);
        size += 1 + EncodingUtils.encodedShortStringLength("object-bool") + EncodingUtils.encodedBooleanLength();
        assertEquals(size, result.getEncodedSize());

        result.setObject("object-byte", Byte.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("object-byte") + EncodingUtils.encodedByteLength();
        assertEquals(size, result.getEncodedSize());

        result.setObject("object-bytes", _bytes);
        size += 1 + EncodingUtils.encodedShortStringLength("object-bytes") + 4 + _bytes.length;
        assertEquals(size, result.getEncodedSize());

        result.setObject("object-char", 'c');
        size += 1 + EncodingUtils.encodedShortStringLength("object-char") + EncodingUtils.encodedCharLength();
        assertEquals(size, result.getEncodedSize());

        result.setObject("object-double", Double.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("object-double") + EncodingUtils.encodedDoubleLength();
        assertEquals(size, result.getEncodedSize());

        result.setObject("object-float", Float.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("object-float") + EncodingUtils.encodedFloatLength();
        assertEquals(size, result.getEncodedSize());

        result.setObject("object-int", Integer.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("object-int") + EncodingUtils.encodedIntegerLength();
        assertEquals(size, result.getEncodedSize());

        result.setObject("object-long", Long.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("object-long") + EncodingUtils.encodedLongLength();
        assertEquals(size, result.getEncodedSize());

        result.setObject("object-short", Short.MAX_VALUE);
        size += 1 + EncodingUtils.encodedShortStringLength("object-short") + EncodingUtils.encodedShortLength();
        assertEquals(size, result.getEncodedSize());
    }

    /**
     * Additional test for setObject
     */
    @Test
    public void testSetObject()
    {
        FieldTable table = new FieldTable();

        // Try setting a non primative object

        try
        {
            table.setObject("value", this);
            fail("Only primative values allowed in setObject");
        }
        catch (AMQPInvalidClassException iae)
        {
            // normal path
        }
        // so length should be zero
        assertEquals(0, table.getEncodedSize());
    }

    /**
     * Additional test checkPropertyName doesn't accept Null
     */
    @Test
    public void testCheckPropertyNameasNull()
    {
        FieldTable table = new FieldTable();

        try
        {
            table.setObject((String) null, "String");
            fail("Null property name is not allowed");
        }
        catch (IllegalArgumentException iae)
        {
            // normal path
        }
        // so length should be zero
        assertEquals(0, table.getEncodedSize());
    }

    /**
     * Additional test checkPropertyName doesn't accept an empty String
     */
    @Test
    public void testCheckPropertyNameasEmptyString()
    {
        FieldTable table = new FieldTable();

        try
        {
            table.setObject("", "String");
            fail("empty property name is not allowed");
        }
        catch (IllegalArgumentException iae)
        {
            // normal path
        }
        // so length should be zero
        assertEquals(0, table.getEncodedSize());
    }

    /**
     * Additional test checkPropertyName doesn't accept an empty String
     */
    @Test
    public void testCheckPropertyNamehasMaxLength()
    {
        FieldTable table = new FieldTable(true);

        StringBuffer longPropertyName = new StringBuffer(129);

        for (int i = 0; i < 129; i++)
        {
            longPropertyName.append("x");
        }

        try
        {
            table.setObject(longPropertyName.toString(), "String");
            fail("property name must be < 128 characters");
        }
        catch (IllegalArgumentException iae)
        {
            // normal path
        }
        // so length should be zero
        assertEquals(0, table.getEncodedSize());
    }

    /**
     * Additional test checkPropertyName starts with a letter
     */
    @Test
    public void testCheckPropertyNameStartCharacterIsLetter()
    {
        FieldTable table = new FieldTable(true);

        // Try a name that starts with a number
        try
        {
            table.setObject("1", "String");
            fail("property name must start with a letter");
        }
        catch (IllegalArgumentException iae)
        {
            // normal path
        }
        // so length should be zero
        assertEquals(0, table.getEncodedSize());
    }

    /**
     * Additional test checkPropertyName starts with a hash or a dollar
     */
    @Test
    public void testCheckPropertyNameStartCharacterIsHashorDollar()
    {
        FieldTable table = new FieldTable(true);

        // Try a name that starts with a number
        try
        {
            table.setObject("#", "String");
            table.setObject("$", "String");
        }
        catch (IllegalArgumentException iae)
        {
            fail("property name are allowed to start with # and $s");
        }
    }

    /**
     * Additional test to test the contents of the table
     */
    @Test
    public void testContents()
    {
        FieldTable table = new FieldTable();

        table.setObject("StringProperty", "String");

        assertEquals("String", table.getString("StringProperty"));

        // Test Clear

        table.clear();

        checkEmpty(table);
    }

    /**
     * Test the contents of the sets
     */
    @Test
    public void testSets()
    {

        FieldTable table = new FieldTable();

        table.setObject("n1", "1");
        table.setObject("n2", "2");
        table.setObject("n3", "3");

        assertEquals("1", table.getObject("n1"));
        assertEquals("2", table.getObject("n2"));
        assertEquals("3", table.getObject("n3"));
    }

    @Test
    public void testAddAll()
    {
        final FieldTable table1 = new FieldTable();
        table1.setInteger("int1", 1);
        table1.setInteger("int2", 2);
        assertEquals("Unexpected number of entries in table1", (long) 2, (long) table1.size());

        final FieldTable table2 = new FieldTable();
        table2.setInteger("int3", 3);
        table2.setInteger("int4", 4);
        assertEquals("Unexpected number of entries in table2", (long) 2, (long) table2.size());

        table1.addAll(table2);
        assertEquals("Unexpected number of entries in table1 after addAll", (long) 4, (long) table1.size());
        assertEquals(Integer.valueOf(3), table1.getInteger("int3"));
    }

    @Test
    public void testAddAllWithEmptyFieldTable()
    {
        final FieldTable table1 = new FieldTable();
        table1.setInteger("int1", 1);
        table1.setInteger("int2", 2);
        assertEquals("Unexpected number of entries in table1", (long) 2, (long) table1.size());

        final FieldTable emptyFieldTable = new FieldTable();

        table1.addAll(emptyFieldTable);
        assertEquals("Unexpected number of entries in table1 after addAll", (long) 2, (long) table1.size());
    }

    /**
     * Tests that when copying properties into a new FielTable using the addAll() method, the
     *  properties are successfully added to the destination table when the source FieldTable
     * was created from encoded input bytes,
     */
    @Test
    public void testAddingAllFromFieldTableCreatedUsingEncodedBytes() throws Exception
    {
        String myBooleanTestProperty = "myBooleanTestProperty";

        //Create a new FieldTable and use it to encode data into a byte array.
        FieldTable encodeTable = new FieldTable();
        encodeTable.setObject(myBooleanTestProperty, true);
        byte[] data = encodeTable.getDataAsBytes();
        int length = data.length;

        //Verify we got the expected mount of encoded data (1B type hdr + 21B for name + 1B type hdr + 1B for boolean)
        assertEquals("unexpected data length", (long) 24, (long) length);

        //Create a second FieldTable from the encoded bytes
        FieldTable tableFromBytes = new FieldTable(QpidByteBuffer.wrap(data));

        //Create a final FieldTable and addAll() from the table created with encoded bytes
        FieldTable destinationTable = new FieldTable();
        assertTrue("unexpected size", destinationTable.isEmpty());
        destinationTable.addAll(tableFromBytes);

        //Verify that the destination table now contains the expected entry
        assertEquals("unexpected size", (long) 1, (long) destinationTable.size());
        assertTrue("expected property not present", destinationTable.containsKey(myBooleanTestProperty));
        assertTrue("unexpected property value", destinationTable.getBoolean(myBooleanTestProperty));
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
