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

package org.apache.qpid.server.bytebuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.Primitives;

import org.apache.qpid.test.utils.UnitTestBase;

public class QpidByteBufferTest extends UnitTestBase
{
    private static final int BUFFER_FRAGMENT_SIZE = 5;
    private static final int BUFFER_SIZE = 10;
    private static final int POOL_SIZE = 20;
    private static final double SPARSITY_FRACTION = 0.5;


    private QpidByteBuffer _slicedBuffer;
    private QpidByteBuffer _parent;

    @Before
    public void setUp() throws Exception
    {
        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(BUFFER_FRAGMENT_SIZE, POOL_SIZE, SPARSITY_FRACTION);
        _parent = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
        }
        finally
        {
            if (_parent != null)
            {
                _parent.dispose();
            }
            if (_slicedBuffer != null)
            {
                _slicedBuffer.dispose();
            }
            QpidByteBuffer.deinitialisePool();
        }
    }

    @Test
    public void testPutGetByIndex() throws Exception
    {
        testPutGetByIndex(double.class, 1.0);
        testPutGetByIndex(float.class, 1.0f);
        testPutGetByIndex(long.class, 1L);
        testPutGetByIndex(int.class, 1);
        testPutGetByIndex(char.class, 'A');
        testPutGetByIndex(short.class, (short)1);
        testPutGetByIndex(byte.class, (byte)1);
    }

    @Test
    public void testPutGet() throws Exception
    {
        testPutGet(double.class, false, 1.0);
        testPutGet(float.class, false, 1.0f);
        testPutGet(long.class, false, 1L);
        testPutGet(int.class, false, 1);
        testPutGet(char.class, false, 'A');
        testPutGet(short.class, false, (short)1);
        testPutGet(byte.class, false, (byte)1);

        testPutGet(int.class, true, 1L);
        testPutGet(short.class, true, 1);
        testPutGet(byte.class, true, (short)1);
    }

    @Test
    public void testMarkReset() throws Exception
    {
        _slicedBuffer = createSlice();

        _slicedBuffer.mark();
        _slicedBuffer.position(_slicedBuffer.position() + 1);
        assertEquals("Unexpected position after move", (long) 1, (long) _slicedBuffer.position());

        _slicedBuffer.reset();
        assertEquals("Unexpected position after reset", (long) 0, (long) _slicedBuffer.position());
    }

    @Test
    public void testMarkResetAcrossFragmentBoundary() throws Exception
    {
        for (int i = 0; i < BUFFER_SIZE; ++i)
        {
            _parent.put((byte) i);
        }
        _parent.flip();
        _parent.mark();
        for (int i = 0; i < BUFFER_FRAGMENT_SIZE + 2; ++i)
        {
            assertEquals("Unexpected value", (long) i, (long) _parent.get());
        }
        _parent.reset();
        for (int i = 0; i < BUFFER_SIZE; ++i)
        {
            assertEquals("Unexpected value", (long) i, (long) _parent.get());
        }
    }

    @Test
    public void testPosition() throws Exception
    {
        _slicedBuffer = createSlice();

        assertEquals("Unexpected position for new slice", (long) 0, (long) _slicedBuffer.position());

        _slicedBuffer.position(1);
        assertEquals("Unexpected position after advance", (long) 1, (long) _slicedBuffer.position());

        final int oldLimit = _slicedBuffer.limit();
        _slicedBuffer.limit(oldLimit - 1);
        try
        {
            _slicedBuffer.position(oldLimit);
            fail("Exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testSettingPositionBackwardsResetsMark()
    {
        _parent.position(8);
        _parent.mark();
        _parent.position(7);
        try
        {
            _parent.reset();
            fail("Expected exception not thrown");
        }
        catch (InvalidMarkException e)
        {
            // pass
        }
    }

    @Test
    public void testSettingPositionForwardDoeNotResetMark()
    {
        final int originalPosition = 3;
        _parent.position(originalPosition);
        _parent.mark();
        _parent.position(9);

        _parent.reset();

        assertEquals("Unexpected position", (long) originalPosition, (long) _parent.position());
    }

    @Test
    public void testRewind() throws Exception
    {
        final int expectedLimit = 7;
        _parent.position(1);
        _parent.limit(expectedLimit);
        _parent.mark();
        _parent.position(3);

        _parent.rewind();

        assertEquals("Unexpected position", (long) 0, (long) _parent.position());
        assertEquals("Unexpected limit", (long) expectedLimit, (long) _parent.limit());
        try
        {
            _parent.reset();
            fail("Expected exception not thrown");
        }
        catch (InvalidMarkException e)
        {
            // pass
        }
    }

    @Test
    public void testBulkPutGet() throws Exception
    {
        _slicedBuffer = createSlice();

        final byte[] source = getTestBytes(_slicedBuffer.remaining());

        QpidByteBuffer rv = _slicedBuffer.put(source, 0, source.length);
        assertEquals("Unexpected builder return value", _slicedBuffer, rv);

        _slicedBuffer.flip();
        byte[] target = new byte[_slicedBuffer.remaining()];
        rv = _slicedBuffer.get(target, 0, target.length);
        assertEquals("Unexpected builder return value", _slicedBuffer, rv);

        Assert.assertArrayEquals("Unexpected bulk put/get result", source, target);


        _slicedBuffer.clear();
        _slicedBuffer.position(1);

        try
        {
            _slicedBuffer.put(source, 0, source.length);
            fail("Exception not thrown");
        }
        catch (BufferOverflowException e)
        {
            // pass
        }

        assertEquals("Position should be unchanged after failed put", (long) 1, (long) _slicedBuffer.position());

        try
        {
            _slicedBuffer.get(target, 0, target.length);
            fail("Exception not thrown");
        }
        catch (BufferUnderflowException e)
        {
            // pass
        }

        assertEquals("Position should be unchanged after failed get", (long) 1, (long) _slicedBuffer.position());
    }

    @Test
    public void testPutQpidByteBufferMultipleIntoMultiple() throws Exception
    {
        _slicedBuffer = createSlice();

        final byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);
        _slicedBuffer.flip();

        try( QpidByteBuffer other = QpidByteBuffer.allocateDirect(BUFFER_SIZE))
        {
            other.put(_slicedBuffer);

            other.flip();
            byte[] target = new byte[other.remaining()];
            other.get(target);
            Assert.assertArrayEquals("Unexpected put QpidByteBuffer result", source, target);
        }
    }

    @Test
    public void testPutQpidByteBufferMultipleIntoSingle() throws Exception
    {
        _slicedBuffer = createSlice();

        final byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);
        _slicedBuffer.flip();

        try( QpidByteBuffer other = QpidByteBuffer.wrap(new byte[source.length]))
        {
            other.put(_slicedBuffer);

            other.flip();
            byte[] target = new byte[other.remaining()];
            other.get(target);
            Assert.assertArrayEquals("Unexpected put QpidByteBuffer result", source, target);
        }
    }

    @Test
    public void testPutQpidByteBufferSingleIntoMultiple() throws Exception
    {
        _slicedBuffer = createSlice();

        final byte[] source = getTestBytes(_slicedBuffer.remaining());

        try( QpidByteBuffer other = QpidByteBuffer.wrap(source))
        {
            _slicedBuffer.put(other);

            _slicedBuffer.flip();
            byte[] target = new byte[_slicedBuffer.remaining()];
            _slicedBuffer.get(target);
            Assert.assertArrayEquals("Unexpected put QpidByteBuffer result", source, target);
        }
    }

    @Test
    public void testPutByteBuffer() throws Exception
    {
        final byte[] source = getTestBytes(_parent.remaining() - 1);

        ByteBuffer src = ByteBuffer.wrap(source);

        _parent.put(src);

        _parent.flip();
        byte[] target = new byte[_parent.remaining()];
        _parent.get(target);
        Assert.assertArrayEquals("Unexpected but ByteBuffer result", source, target);
    }

    @Test
    public void testDuplicate()
    {
        _slicedBuffer = createSlice();
        _slicedBuffer.position(1);
        int originalLimit = _slicedBuffer.limit();
        _slicedBuffer.limit(originalLimit - 1);

        try (QpidByteBuffer duplicate = _slicedBuffer.duplicate())
        {
            assertEquals("Unexpected position", (long) _slicedBuffer.position(), (long) duplicate.position());
            assertEquals("Unexpected limit", (long) _slicedBuffer.limit(), (long) duplicate.limit());
            assertEquals("Unexpected capacity", (long) _slicedBuffer.capacity(), (long) duplicate.capacity());

            duplicate.position(2);
            duplicate.limit(originalLimit - 2);

            assertEquals("Unexpected position in the original", (long) 1, (long) _slicedBuffer.position());
            assertEquals("Unexpected limit in the original",
                                (long) (originalLimit - 1),
                                (long) _slicedBuffer.limit());
        }
    }

    @Test
    public void testCopyToByteBuffer()
    {
        _slicedBuffer = createSlice();
        byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);
        _slicedBuffer.flip();

        int originalRemaining = _slicedBuffer.remaining();
        ByteBuffer destination =  ByteBuffer.allocate(source.length);
        _slicedBuffer.copyTo(destination);

        assertEquals("Unexpected remaining in original QBB",
                            (long) originalRemaining,
                            (long) _slicedBuffer.remaining());
        assertEquals("Unexpected remaining in destination", (long) 0, (long) destination.remaining());

        Assert.assertArrayEquals("Unexpected copyTo result", source, destination.array());
    }

    @Test
    public void testCopyToArray()
    {
        _slicedBuffer = createSlice();
        byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);
        _slicedBuffer.flip();

        int originalRemaining = _slicedBuffer.remaining();
        byte[] destination = new byte[source.length];
        _slicedBuffer.copyTo(destination);

        assertEquals("Unexpected remaining in original QBB",
                            (long) originalRemaining,
                            (long) _slicedBuffer.remaining());

        Assert.assertArrayEquals("Unexpected copyTo result", source, destination);
    }

    @Test
    public void testPutCopyOfSingleIntoMultiple()
    {
        _slicedBuffer = createSlice();
        byte[] source = getTestBytes(_slicedBuffer.remaining());

        QpidByteBuffer sourceQpidByteBuffer =  QpidByteBuffer.wrap(source);
        _slicedBuffer.putCopyOf(sourceQpidByteBuffer);

        assertEquals("Copied buffer should not be changed",
                            (long) source.length,
                            (long) sourceQpidByteBuffer.remaining());
        assertEquals("Buffer should be full", (long) 0, (long) _slicedBuffer.remaining());
        _slicedBuffer.flip();

        byte[] destination = new byte[source.length];
        _slicedBuffer.get(destination);

        Assert.assertArrayEquals("Unexpected putCopyOf result", source, destination);
    }

    @Test
    public void testPutCopyOfMultipleIntoSingle()
    {
        _slicedBuffer = createSlice();
        byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);
        _slicedBuffer.flip();

        try (QpidByteBuffer target = QpidByteBuffer.wrap(new byte[source.length + 1]))
        {
            target.putCopyOf(_slicedBuffer);

            assertEquals("Copied buffer should not be changed",
                                (long) source.length,
                                (long) _slicedBuffer.remaining());
            assertEquals("Unexpected remaining", (long) 1, (long) target.remaining());
            target.flip();

            byte[] destination = new byte[source.length];
            target.get(destination);

            Assert.assertArrayEquals("Unexpected putCopyOf result", source, destination);
        }
    }

    @Test
    public void testPutCopyOfMultipleIntoMultiple()
    {
        _slicedBuffer = createSlice();
        byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);
        _slicedBuffer.flip();

        try (QpidByteBuffer target = QpidByteBuffer.allocateDirect(BUFFER_SIZE))
        {
            target.putCopyOf(_slicedBuffer);

            assertEquals("Copied buffer should not be changed",
                                (long) source.length,
                                (long) _slicedBuffer.remaining());
            assertEquals("Unexpected remaining", (long) 2, (long) target.remaining());
            target.flip();

            byte[] destination = new byte[source.length];
            target.get(destination);

            Assert.assertArrayEquals("Unexpected putCopyOf result", source, destination);
        }
    }

    @Test
    public void testCompact()
    {
        _slicedBuffer = createSlice();
        byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);

        _slicedBuffer.position(1);
        _slicedBuffer.limit(_slicedBuffer.limit() - 1);

        int remaining =  _slicedBuffer.remaining();
        _slicedBuffer.compact();

        assertEquals("Unexpected position", (long) remaining, (long) _slicedBuffer.position());
        assertEquals("Unexpected limit", (long) _slicedBuffer.capacity(), (long) _slicedBuffer.limit());

        _slicedBuffer.flip();


        byte[] destination =  new byte[_slicedBuffer.remaining()];
        _slicedBuffer.get(destination);

        byte[] expected =  new byte[source.length - 2];
        System.arraycopy(source, 1, expected, 0, expected.length);

        Assert.assertArrayEquals("Unexpected compact result", expected, destination);
    }

    @Test
    public void testSliceOfSlice()
    {
        _slicedBuffer = createSlice();
        byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);

        _slicedBuffer.position(1);
        _slicedBuffer.limit(_slicedBuffer.limit() - 1);

        int remaining = _slicedBuffer.remaining();

        try (QpidByteBuffer newSlice = _slicedBuffer.slice())
        {
            assertEquals("Unexpected position in original", (long) 1, (long) _slicedBuffer.position());
            assertEquals("Unexpected limit in original",
                                (long) (source.length - 1),
                                (long) _slicedBuffer.limit());
            assertEquals("Unexpected position", (long) 0, (long) newSlice.position());
            assertEquals("Unexpected limit", (long) remaining, (long) newSlice.limit());
            assertEquals("Unexpected capacity", (long) remaining, (long) newSlice.capacity());

            byte[] destination =  new byte[newSlice.remaining()];
            newSlice.get(destination);

            byte[] expected =  new byte[source.length - 2];
            System.arraycopy(source, 1, expected, 0, expected.length);
            Assert.assertArrayEquals("Unexpected slice result", expected, destination);
        }
    }

    @Test
    public void testViewOfSlice()
    {
        _slicedBuffer = createSlice();
        byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);

        _slicedBuffer.position(1);
        _slicedBuffer.limit(_slicedBuffer.limit() - 1);

        try (QpidByteBuffer view = _slicedBuffer.view(0, _slicedBuffer.remaining()))
        {
            assertEquals("Unexpected position in original", (long) 1, (long) _slicedBuffer.position());
            assertEquals("Unexpected limit in original",
                                (long) (source.length - 1),
                                (long) _slicedBuffer.limit());

            assertEquals("Unexpected position", (long) 0, (long) view.position());
            assertEquals("Unexpected limit", (long) _slicedBuffer.remaining(), (long) view.limit());
            assertEquals("Unexpected capacity", (long) _slicedBuffer.remaining(), (long) view.capacity());

            byte[] destination =  new byte[view.remaining()];
            view.get(destination);

            byte[] expected =  new byte[source.length - 2];
            System.arraycopy(source, 1, expected, 0, expected.length);
            Assert.assertArrayEquals("Unexpected view result", expected, destination);
        }

        try (QpidByteBuffer view = _slicedBuffer.view(1, _slicedBuffer.remaining() - 2))
        {
            assertEquals("Unexpected position in original", (long) 1, (long) _slicedBuffer.position());
            assertEquals("Unexpected limit in original",
                                (long) (source.length - 1),
                                (long) _slicedBuffer.limit());

            assertEquals("Unexpected position", (long) 0, (long) view.position());
            assertEquals("Unexpected limit", (long) (_slicedBuffer.remaining() - 2), (long) view.limit());
            assertEquals("Unexpected capacity", (long) (_slicedBuffer.remaining() - 2), (long) view.capacity());

            byte[] destination =  new byte[view.remaining()];
            view.get(destination);

            byte[] expected =  new byte[source.length - 4];
            System.arraycopy(source, 2, expected, 0, expected.length);
            Assert.assertArrayEquals("Unexpected view result", expected, destination);
        }
    }

    @Test
    public void testAsInputStream() throws Exception
    {
        _slicedBuffer = createSlice();
        byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);

        _slicedBuffer.position(1);
        _slicedBuffer.limit(_slicedBuffer.limit() - 1);

        ByteArrayOutputStream destination = new ByteArrayOutputStream();
        try(InputStream is = _slicedBuffer.asInputStream())
        {
            ByteStreams.copy(is, destination);
        }

        byte[] expected =  new byte[source.length - 2];
        System.arraycopy(source, 1, expected, 0, expected.length);
        Assert.assertArrayEquals("Unexpected view result", expected, destination.toByteArray());
    }

    private byte[] getTestBytes(final int length)
    {
        final byte[] source = new byte[length];
        for (int i = 0; i < source.length; i++)
        {
            source[i] = (byte) ('A' + i);
        }
        return source;
    }

    private QpidByteBuffer createSlice()
    {
        _parent.position(1);
        _parent.limit(_parent.capacity() - 1);

        return _parent.slice();
    }

    private void testPutGet(final Class<?> primitiveTargetClass, final boolean unsigned, final Object value) throws Exception
    {
        int size = sizeof(primitiveTargetClass);

        _parent.position(1);
        _parent.limit(size + 1);

        _slicedBuffer = _parent.slice();
        _parent.limit(_parent.capacity());

        assertEquals("Unexpected position ", (long) 0, (long) _slicedBuffer.position());
        assertEquals("Unexpected limit ", (long) size, (long) _slicedBuffer.limit());
        assertEquals("Unexpected capacity ", (long) size, (long) _slicedBuffer.capacity());

        String methodSuffix = getMethodSuffix(primitiveTargetClass, unsigned);
        Method put = _slicedBuffer.getClass().getMethod("put" + methodSuffix, Primitives.primitiveTypeOf(value.getClass()));
        Method get = _slicedBuffer.getClass().getMethod("get" + methodSuffix);


        _slicedBuffer.mark();
        QpidByteBuffer rv = (QpidByteBuffer) put.invoke(_slicedBuffer, value);
        assertEquals("Unexpected builder return value for type " + methodSuffix, _slicedBuffer, rv);

        assertEquals("Unexpected position for type " + methodSuffix,
                            (long) size,
                            (long) _slicedBuffer.position());

        try
        {
            invokeMethod(put, value);
            fail("BufferOverflowException should be thrown for put with insufficient room for " + methodSuffix);
        }
        catch (BufferOverflowException e)
        {
            // pass
        }

        _slicedBuffer.reset();

        assertEquals("Unexpected position after reset", (long) 0, (long) _slicedBuffer.position());

        Object retrievedValue = get.invoke(_slicedBuffer);
        assertEquals("Unexpected value retrieved from get method for " + methodSuffix, value, retrievedValue);
        try
        {
            invokeMethod(get);
            fail("BufferUnderflowException not thrown for get with insufficient room for " + methodSuffix);
        }
        catch (BufferUnderflowException ite)
        {
            // pass
        }
        _slicedBuffer.dispose();
        _slicedBuffer = null;
    }

    private void testPutGetByIndex(final Class<?> primitiveTargetClass, Object value) throws Exception
    {
        int size = sizeof(primitiveTargetClass);

        _parent.position(1);
        _parent.limit(size + 1);

        _slicedBuffer = _parent.slice();
        _parent.limit(_parent.capacity());

        String methodSuffix = getMethodSuffix(primitiveTargetClass, false);
        Method put = _slicedBuffer.getClass().getMethod("put" + methodSuffix, int.class, primitiveTargetClass);
        Method get = _slicedBuffer.getClass().getMethod("get" + methodSuffix, int.class);

        QpidByteBuffer rv = (QpidByteBuffer) put.invoke(_slicedBuffer, 0, value);
        assertEquals("Unexpected builder return value for type " + methodSuffix, _slicedBuffer, rv);

        Object retrievedValue = get.invoke(_slicedBuffer, 0);
        assertEquals("Unexpected value retrieved from index get method for " + methodSuffix,
                            value,
                            retrievedValue);

        try
        {
            invokeMethod(put, 1, value);
            fail("IndexOutOfBoundsException not thrown for  indexed " + methodSuffix + " put");
        }
        catch (IndexOutOfBoundsException ite)
        {
            // pass
        }

        try
        {
            invokeMethod(put, -1, value);
            fail("IndexOutOfBoundsException not thrown for indexed " + methodSuffix + " put with negative index");
        }
        catch (IndexOutOfBoundsException ite)
        {
            // pass
        }

        try
        {
            invokeMethod(get, 1);
            fail("IndexOutOfBoundsException not thrown for indexed " + methodSuffix + " get");
        }
        catch (IndexOutOfBoundsException ite)
        {
            // pass
        }

        try
        {
            invokeMethod(get, -1);
            fail("IndexOutOfBoundsException not thrown for indexed " + methodSuffix + " get with negative index");
        }
        catch (IndexOutOfBoundsException ite)
        {
            // pass
        }
        _slicedBuffer.dispose();
        _slicedBuffer = null;
    }

    private void invokeMethod(final Method method, final Object... value)
            throws Exception
    {
        try
        {
            method.invoke(_slicedBuffer, value);
        }
        catch (InvocationTargetException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof Exception)
            {
                throw (Exception)cause;
            }
            fail(String.format("Unexpected throwable on method %s invocation: %s", method.getName(), cause));
        }
    }


    private String getMethodSuffix(final Class<?> target, final boolean unsigned)
    {
        StringBuilder name = new StringBuilder();
        if (unsigned)
        {
            name.append("Unsigned");
        }
        if ((!target.isAssignableFrom(byte.class) || unsigned))
        {
            String simpleName = target.getSimpleName();
            name.append(simpleName.substring(0, 1).toUpperCase()).append(simpleName.substring(1));
        }

        return name.toString();
    }

    private int sizeof(final Class<?> type)
    {
        if (type.isAssignableFrom(double.class))
        {
            return 8;
        }
        else if (type.isAssignableFrom(float.class))
        {
            return 4;
        }
        else if (type.isAssignableFrom(long.class))
        {
            return 8;
        }
        else if (type.isAssignableFrom(int.class))
        {
            return 4;
        }
        else if (type.isAssignableFrom(short.class))
        {
            return 2;
        }
        else if (type.isAssignableFrom(char.class))
        {
            return 2;
        }
        else if (type.isAssignableFrom(byte.class))
        {
            return 1;
        }
        else
        {
            throw new UnsupportedOperationException("Unexpected type " + type);
        }
    }

    @Test
    public void testPooledBufferIsZeroedLoan() throws Exception
    {
        try (QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(BUFFER_SIZE))
        {
            buffer.put((byte) 0xFF);
        }

        try (QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(BUFFER_SIZE))
        {
            buffer.limit(1);
            assertEquals("Pooled QpidByteBuffer is not zeroed.", (long) (byte) 0x0, (long) buffer.get());
        }
    }

    @Test
    public void testAllocateDirectOfSameSize() throws Exception
    {
        int bufferSize = BUFFER_SIZE;
        try (QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(bufferSize))
        {
            assertEquals("Unexpected buffer size", (long) bufferSize, (long) buffer.capacity());
            assertEquals("Unexpected position on newly created buffer", (long) 0, (long) buffer.position());
            assertEquals("Unexpected limit on newly created buffer", (long) bufferSize, (long) buffer.limit());
        }
    }

    @Test
    public void testAllocateDirectOfSmallerSize() throws Exception
    {
        int bufferSize = BUFFER_SIZE - 1;
        try (QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(bufferSize))
        {
            assertEquals("Unexpected buffer size", (long) bufferSize, (long) buffer.capacity());
            assertEquals("Unexpected position on newly created buffer", (long) 0, (long) buffer.position());
            assertEquals("Unexpected limit on newly created buffer", (long) bufferSize, (long) buffer.limit());
        }
    }

    @Test
    public void testAllocateDirectOfLargerSize() throws Exception
    {
        int bufferSize = BUFFER_SIZE + 1;
        try (QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(bufferSize))
        {
            assertEquals("Unexpected buffer size", (long) bufferSize, (long) buffer.capacity());
            assertEquals("Unexpected position on newly created buffer", (long) 0, (long) buffer.position());
            assertEquals("Unexpected limit on newly created buffer", (long) bufferSize, (long) buffer.limit());
        }
    }

    @Test
    public void testAllocateDirectWithNegativeSize() throws Exception
    {
        try
        {
            QpidByteBuffer.allocateDirect(-1);
            fail("It is not legal to create buffer with negative size.");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testSettingUpPoolTwice() throws Exception
    {
        try
        {
            QpidByteBuffer.initialisePool(BUFFER_SIZE + 1, POOL_SIZE + 1, SPARSITY_FRACTION);
            fail("It is not legal to initialize buffer twice with different settings.");
        }
        catch (IllegalStateException e)
        {
            // pass
        }
    }

    @Test
    public void testDeflateInflateDirect() throws Exception
    {
        byte[] input = "aaabbbcccddddeeeffff".getBytes();
        try (QpidByteBuffer inputBuf = QpidByteBuffer.allocateDirect(input.length))
        {
            inputBuf.put(input);
            inputBuf.flip();

            assertEquals((long) input.length, (long) inputBuf.remaining());

            doDeflateInflate(input, inputBuf, true);
        }
    }

    @Test
    public void testDeflateInflateHeap() throws Exception
    {
        byte[] input = "aaabbbcccddddeeeffff".getBytes();

        try (QpidByteBuffer buffer = QpidByteBuffer.wrap(input))
        {
            doDeflateInflate(input, buffer, false);
        }
    }

    @Test
    public void testInflatingUncompressedBytes_ThrowsZipException() throws Exception
    {
        byte[] input = "not_a_compressed_stream".getBytes();

        try (QpidByteBuffer original = QpidByteBuffer.wrap(input))
        {
            QpidByteBuffer.inflate(original);
            fail("Exception not thrown");
        }
        catch(java.util.zip.ZipException ze)
        {
            // PASS
        }
    }

    @Test
    public void testSlice() throws Exception
    {
        try (QpidByteBuffer directBuffer = QpidByteBuffer.allocate(true, 6))
        {
            directBuffer.position(2);
            directBuffer.limit(5);
            try (QpidByteBuffer directSlice = directBuffer.slice())
            {
                assertTrue("Direct slice should be direct too", directSlice.isDirect());
                assertEquals("Unexpected capacity", (long) 3, (long) directSlice.capacity());
                assertEquals("Unexpected limit", (long) 3, (long) directSlice.limit());
                assertEquals("Unexpected position", (long) 0, (long) directSlice.position());
            }
        }
    }

    @Test
    public void testView() throws Exception
    {
        byte[] content = "ABCDEF".getBytes();
        try (QpidByteBuffer buffer = QpidByteBuffer.allocate(true, content.length))
        {
            buffer.put(content);
            buffer.position(2);
            buffer.limit(5);

            try (QpidByteBuffer view = buffer.view(0, buffer.remaining()))
            {
                assertTrue("Unexpected view direct", view.isDirect());
                assertEquals("Unexpected capacity", (long) 3, (long) view.capacity());
                assertEquals("Unexpected limit", (long) 3, (long) view.limit());
                assertEquals("Unexpected position", (long) 0, (long) view.position());

                final byte[] destination = new byte[view.remaining()];
                view.get(destination);
                Assert.assertArrayEquals("CDE".getBytes(), destination);
            }

            try (QpidByteBuffer viewWithOffset = buffer.view(1, 1))
            {
                final byte[] destination = new byte[viewWithOffset.remaining()];
                viewWithOffset.get(destination);
                Assert.assertArrayEquals("D".getBytes(), destination);
            }
        }
    }

    @Test
    public void testSparsity()
    {
        assertFalse("Unexpected sparsity after creation", _parent.isSparse());
        QpidByteBuffer child = _parent.view(0, 8);
        QpidByteBuffer grandChild = child.view(0, 2);

        assertFalse("Unexpected sparsity after child creation", _parent.isSparse());
        _parent.dispose();
        _parent = null;

        assertFalse("Unexpected sparsity after parent disposal", child.isSparse());

        child.dispose();
        assertTrue("Buffer should be sparse", grandChild.isSparse());
        grandChild.dispose();
    }

    @Test
    public void testAsQpidByteBuffers() throws IOException
    {
        byte[] dataForTwoBufs = "01234567890".getBytes(StandardCharsets.US_ASCII);
        try (QpidByteBuffer qpidByteBuffer = QpidByteBuffer.asQpidByteBuffer(new ByteArrayInputStream(dataForTwoBufs)))
        {
            assertEquals("Unexpected remaining in buf", (long) 11, (long) qpidByteBuffer.remaining());
        }

        try (QpidByteBuffer bufsForEmptyBytes = QpidByteBuffer.asQpidByteBuffer(new ByteArrayInputStream(new byte[]{})))
        {
            assertEquals("Unexpected remaining in buf for empty buffer",
                                (long) 0,
                                (long) bufsForEmptyBytes.remaining());
        }
    }

    @Test
    public void testConcatenate() throws Exception
    {
        try (QpidByteBuffer buffer1 = QpidByteBuffer.allocateDirect(10);
             QpidByteBuffer buffer2 = QpidByteBuffer.allocateDirect(10))
        {
            final short buffer1Value = (short) (1 << 15);
            buffer1.putShort(buffer1Value);
            buffer1.flip();
            final char buffer2Value = 'x';
            buffer2.putChar(2, buffer2Value);
            buffer2.position(4);
            buffer2.flip();

            try (QpidByteBuffer concatenate = QpidByteBuffer.concatenate(buffer1, buffer2))
            {
                assertEquals("Unexpected capacity", (long) 6, (long) concatenate.capacity());
                assertEquals("Unexpected position", (long) 0, (long) concatenate.position());
                assertEquals("Unexpected limit", (long) 6, (long) concatenate.limit());
                assertEquals("Unexpected value 1", (long) buffer1Value, (long) concatenate.getShort());
                assertEquals("Unexpected value 2", (long) buffer2Value, (long) concatenate.getChar(4));
            }
        }
    }

    private void doDeflateInflate(byte[] input,
                                  QpidByteBuffer inputBuf,
                                  boolean direct) throws IOException
    {
        try (QpidByteBuffer deflatedBuf = QpidByteBuffer.deflate(inputBuf))
        {
            assertNotNull(deflatedBuf);

            try (QpidByteBuffer inflatedBuf = QpidByteBuffer.inflate(deflatedBuf))
            {
                assertNotNull(inflatedBuf);

                int inflatedBytesCount = inflatedBuf.remaining();

                byte[] inflatedBytes = new byte[inflatedBytesCount];
                inflatedBuf.get(inflatedBytes);
                byte[] expectedBytes = Arrays.copyOfRange(input, 0, inflatedBytes.length);
                Assert.assertArrayEquals("Inflated buf has unexpected content", expectedBytes, inflatedBytes);

                assertEquals("Unexpected number of inflated bytes",
                                    (long) input.length,
                                    (long) inflatedBytesCount);
            }
        }
    }

}
