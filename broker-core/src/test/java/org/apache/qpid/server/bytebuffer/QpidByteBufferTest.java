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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @BeforeEach
    public void setUp() throws Exception
    {
        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(BUFFER_FRAGMENT_SIZE, POOL_SIZE, SPARSITY_FRACTION);
        _parent = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
    }

    @AfterEach
    public void tearDown() throws Exception
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
    public void testMarkReset()
    {
        _slicedBuffer = createSlice();

        _slicedBuffer.mark();
        _slicedBuffer.position(_slicedBuffer.position() + 1);
        assertEquals(1, (long) _slicedBuffer.position(), "Unexpected position after move");

        _slicedBuffer.reset();
        assertEquals(0, (long) _slicedBuffer.position(), "Unexpected position after reset");
    }

    @Test
    public void testMarkResetAcrossFragmentBoundary()
    {
        for (int i = 0; i < BUFFER_SIZE; ++i)
        {
            _parent.put((byte) i);
        }
        _parent.flip();
        _parent.mark();
        for (int i = 0; i < BUFFER_FRAGMENT_SIZE + 2; ++i)
        {
            assertEquals(i, (long) _parent.get(), "Unexpected value");
        }
        _parent.reset();
        for (int i = 0; i < BUFFER_SIZE; ++i)
        {
            assertEquals(i, (long) _parent.get(), "Unexpected value");
        }
    }

    @Test
    public void testPosition()
    {
        _slicedBuffer = createSlice();

        assertEquals(0, (long) _slicedBuffer.position(), "Unexpected position for new slice");

        _slicedBuffer.position(1);
        assertEquals(1, (long) _slicedBuffer.position(), "Unexpected position after advance");

        final int oldLimit = _slicedBuffer.limit();
        _slicedBuffer.limit(oldLimit - 1);

        assertThrows(IllegalArgumentException.class, () -> _slicedBuffer.position(oldLimit),"Exception not thrown");
    }

    @Test
    public void testSettingPositionBackwardsResetsMark()
    {
        _parent.position(8);
        _parent.mark();
        _parent.position(7);

        assertThrows(InvalidMarkException.class, () -> _parent.reset(), "Expected exception not thrown");
    }

    @Test
    public void testSettingPositionForwardDoeNotResetMark()
    {
        final int originalPosition = 3;
        _parent.position(originalPosition);
        _parent.mark();
        _parent.position(9);
        _parent.reset();

        assertEquals(originalPosition, (long) _parent.position(), "Unexpected position");
    }

    @Test
    public void testRewind()
    {
        final int expectedLimit = 7;
        _parent.position(1);
        _parent.limit(expectedLimit);
        _parent.mark();
        _parent.position(3);
        _parent.rewind();

        assertEquals(0, (long) _parent.position(), "Unexpected position");
        assertEquals(expectedLimit, (long) _parent.limit(), "Unexpected limit");

        assertThrows(InvalidMarkException.class, () -> _parent.reset(), "Expected exception not thrown");
    }

    @Test
    public void testBulkPutGet()
    {
        _slicedBuffer = createSlice();

        final byte[] source = getTestBytes(_slicedBuffer.remaining());

        QpidByteBuffer rv = _slicedBuffer.put(source, 0, source.length);
        assertEquals(_slicedBuffer, rv, "Unexpected builder return value");

        _slicedBuffer.flip();
        byte[] target = new byte[_slicedBuffer.remaining()];
        rv = _slicedBuffer.get(target, 0, target.length);
        assertEquals(_slicedBuffer, rv, "Unexpected builder return value");

        assertArrayEquals(source, target, "Unexpected bulk put/get result");

        _slicedBuffer.clear();
        _slicedBuffer.position(1);

        assertThrows(BufferOverflowException.class, () -> _slicedBuffer.put(source, 0, source.length),
                "Expected exception not thrown");

        assertEquals(1, (long) _slicedBuffer.position(), "Position should be unchanged after failed put");

        assertThrows(BufferUnderflowException.class,() -> _slicedBuffer.get(target, 0, target.length),
                "Expected exception not thrown");

        assertEquals(1, (long) _slicedBuffer.position(), "Position should be unchanged after failed get");
    }

    @Test
    public void testPutQpidByteBufferMultipleIntoMultiple()
    {
        _slicedBuffer = createSlice();

        final byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);
        _slicedBuffer.flip();

        try (final QpidByteBuffer other = QpidByteBuffer.allocateDirect(BUFFER_SIZE))
        {
            other.put(_slicedBuffer);
            other.flip();
            final byte[] target = new byte[other.remaining()];
            other.get(target);
            assertArrayEquals(source, target, "Unexpected put QpidByteBuffer result");
        }
    }

    @Test
    public void testPutQpidByteBufferMultipleIntoSingle()
    {
        _slicedBuffer = createSlice();

        final byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);
        _slicedBuffer.flip();

        try (final QpidByteBuffer other = QpidByteBuffer.wrap(new byte[source.length]))
        {
            other.put(_slicedBuffer);
            other.flip();
            final byte[] target = new byte[other.remaining()];
            other.get(target);
            assertArrayEquals(source, target, "Unexpected put QpidByteBuffer result");
        }
    }

    @Test
    public void testPutQpidByteBufferSingleIntoMultiple()
    {
        _slicedBuffer = createSlice();

        final byte[] source = getTestBytes(_slicedBuffer.remaining());
        try (final QpidByteBuffer other = QpidByteBuffer.wrap(source))
        {
            _slicedBuffer.put(other);
            _slicedBuffer.flip();
            final byte[] target = new byte[_slicedBuffer.remaining()];
            _slicedBuffer.get(target);
            assertArrayEquals(source, target, "Unexpected put QpidByteBuffer result");
        }
    }

    @Test
    public void testPutByteBuffer()
    {
        final byte[] source = getTestBytes(_parent.remaining() - 1);
        final ByteBuffer src = ByteBuffer.wrap(source);

        _parent.put(src);

        _parent.flip();
        final byte[] target = new byte[_parent.remaining()];
        _parent.get(target);
        assertArrayEquals(source, target, "Unexpected but ByteBuffer result");
    }

    @Test
    public void testDuplicate()
    {
        _slicedBuffer = createSlice();
        _slicedBuffer.position(1);
        final int originalLimit = _slicedBuffer.limit();
        _slicedBuffer.limit(originalLimit - 1);

        try (final QpidByteBuffer duplicate = _slicedBuffer.duplicate())
        {
            assertEquals(_slicedBuffer.position(), (long) duplicate.position(), "Unexpected position");
            assertEquals(_slicedBuffer.limit(), (long) duplicate.limit(), "Unexpected limit");
            assertEquals(_slicedBuffer.capacity(), (long) duplicate.capacity(), "Unexpected capacity");

            duplicate.position(2);
            duplicate.limit(originalLimit - 2);

            assertEquals(1, (long) _slicedBuffer.position(), "Unexpected position in the original");
            assertEquals(originalLimit - 1, (long) _slicedBuffer.limit(), "Unexpected limit in the original");
        }
    }

    @Test
    public void testCopyToByteBuffer()
    {
        _slicedBuffer = createSlice();
        final byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);
        _slicedBuffer.flip();

        final int originalRemaining = _slicedBuffer.remaining();
        final ByteBuffer destination =  ByteBuffer.allocate(source.length);
        _slicedBuffer.copyTo(destination);

        assertEquals(originalRemaining, (long) _slicedBuffer.remaining(), "Unexpected remaining in original QBB");
        assertEquals(0, (long) destination.remaining(), "Unexpected remaining in destination");

        assertArrayEquals(source, destination.array(), "Unexpected copyTo result");
    }

    @Test
    public void testCopyToArray()
    {
        _slicedBuffer = createSlice();
        final byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);
        _slicedBuffer.flip();

        final int originalRemaining = _slicedBuffer.remaining();
        final byte[] destination = new byte[source.length];
        _slicedBuffer.copyTo(destination);

        assertEquals(originalRemaining, (long) _slicedBuffer.remaining(), "Unexpected remaining in original QBB");

        assertArrayEquals(source, destination, "Unexpected copyTo result");
    }

    @Test
    public void testPutCopyOfSingleIntoMultiple()
    {
        _slicedBuffer = createSlice();
        final byte[] source = getTestBytes(_slicedBuffer.remaining());

        final QpidByteBuffer sourceQpidByteBuffer =  QpidByteBuffer.wrap(source);
        _slicedBuffer.putCopyOf(sourceQpidByteBuffer);

        assertEquals(source.length, (long) sourceQpidByteBuffer.remaining(), "Copied buffer should not be changed");
        assertEquals(0, (long) _slicedBuffer.remaining(), "Buffer should be full");
        _slicedBuffer.flip();

        final byte[] destination = new byte[source.length];
        _slicedBuffer.get(destination);

        assertArrayEquals(source, destination, "Unexpected putCopyOf result");
    }

    @Test
    public void testPutCopyOfMultipleIntoSingle()
    {
        _slicedBuffer = createSlice();
        final byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);
        _slicedBuffer.flip();

        try (final QpidByteBuffer target = QpidByteBuffer.wrap(new byte[source.length + 1]))
        {
            target.putCopyOf(_slicedBuffer);

            assertEquals(source.length, (long) _slicedBuffer.remaining(),"Copied buffer should not be changed");
            assertEquals(1, (long) target.remaining(), "Unexpected remaining");
            target.flip();

            final byte[] destination = new byte[source.length];
            target.get(destination);

            assertArrayEquals(source, destination, "Unexpected putCopyOf result");
        }
    }

    @Test
    public void testPutCopyOfMultipleIntoMultiple()
    {
        _slicedBuffer = createSlice();
        final byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);
        _slicedBuffer.flip();

        try (final QpidByteBuffer target = QpidByteBuffer.allocateDirect(BUFFER_SIZE))
        {
            target.putCopyOf(_slicedBuffer);

            assertEquals(source.length, (long) _slicedBuffer.remaining(), "Copied buffer should not be changed");
            assertEquals(2, (long) target.remaining(), "Unexpected remaining");
            target.flip();

            final byte[] destination = new byte[source.length];
            target.get(destination);

            assertArrayEquals(source, destination, "Unexpected putCopyOf result");
        }
    }

    @Test
    public void testCompact()
    {
        _slicedBuffer = createSlice();
        final byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);

        _slicedBuffer.position(1);
        _slicedBuffer.limit(_slicedBuffer.limit() - 1);

        final int remaining =  _slicedBuffer.remaining();
        _slicedBuffer.compact();

        assertEquals(remaining, (long) _slicedBuffer.position(), "Unexpected position");
        assertEquals(_slicedBuffer.capacity(), (long) _slicedBuffer.limit(), "Unexpected limit");

        _slicedBuffer.flip();

        final byte[] destination =  new byte[_slicedBuffer.remaining()];
        _slicedBuffer.get(destination);

        final byte[] expected =  new byte[source.length - 2];
        System.arraycopy(source, 1, expected, 0, expected.length);

        assertArrayEquals(expected, destination, "Unexpected compact result");
    }

    @Test
    public void testSliceOfSlice()
    {
        _slicedBuffer = createSlice();
        final byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);

        _slicedBuffer.position(1);
        _slicedBuffer.limit(_slicedBuffer.limit() - 1);

        final int remaining = _slicedBuffer.remaining();

        try (final QpidByteBuffer newSlice = _slicedBuffer.slice())
        {
            assertEquals(1, (long) _slicedBuffer.position(), "Unexpected position in original");
            assertEquals(source.length - 1, (long) _slicedBuffer.limit(), "Unexpected limit in original");
            assertEquals(0, (long) newSlice.position(), "Unexpected position");
            assertEquals(remaining, (long) newSlice.limit(), "Unexpected limit");
            assertEquals(remaining, (long) newSlice.capacity(), "Unexpected capacity");

            final byte[] destination =  new byte[newSlice.remaining()];
            newSlice.get(destination);

            final byte[] expected =  new byte[source.length - 2];
            System.arraycopy(source, 1, expected, 0, expected.length);
            assertArrayEquals(expected, destination, "Unexpected slice result");
        }
    }

    @Test
    public void testViewOfSlice()
    {
        _slicedBuffer = createSlice();
        final byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);

        _slicedBuffer.position(1);
        _slicedBuffer.limit(_slicedBuffer.limit() - 1);

        try (final QpidByteBuffer view = _slicedBuffer.view(0, _slicedBuffer.remaining()))
        {
            assertEquals(1, (long) _slicedBuffer.position(), "Unexpected position in original");
            assertEquals(source.length - 1, (long) _slicedBuffer.limit(), "Unexpected limit in original");

            assertEquals(0, (long) view.position(), "Unexpected position");
            assertEquals(_slicedBuffer.remaining(), (long) view.limit(), "Unexpected limit");
            assertEquals(_slicedBuffer.remaining(), (long) view.capacity(), "Unexpected capacity");

            final byte[] destination =  new byte[view.remaining()];
            view.get(destination);

            final byte[] expected =  new byte[source.length - 2];
            System.arraycopy(source, 1, expected, 0, expected.length);
            assertArrayEquals(expected, destination, "Unexpected view result");
        }

        try (final QpidByteBuffer view = _slicedBuffer.view(1, _slicedBuffer.remaining() - 2))
        {
            assertEquals(1, (long) _slicedBuffer.position(), "Unexpected position in original");
            assertEquals(source.length - 1, (long) _slicedBuffer.limit(), "Unexpected limit in original");

            assertEquals(0, (long) view.position(), "Unexpected position");
            assertEquals(_slicedBuffer.remaining() - 2, (long) view.limit(), "Unexpected limit");
            assertEquals(_slicedBuffer.remaining() - 2, (long) view.capacity(), "Unexpected capacity");

            final byte[] destination =  new byte[view.remaining()];
            view.get(destination);

            final byte[] expected =  new byte[source.length - 4];
            System.arraycopy(source, 2, expected, 0, expected.length);
            assertArrayEquals(expected, destination, "Unexpected view result");
        }
    }

    @Test
    public void testAsInputStream() throws Exception
    {
        _slicedBuffer = createSlice();
        final byte[] source = getTestBytes(_slicedBuffer.remaining());
        _slicedBuffer.put(source);

        _slicedBuffer.position(1);
        _slicedBuffer.limit(_slicedBuffer.limit() - 1);

        final ByteArrayOutputStream destination = new ByteArrayOutputStream();
        try (final InputStream is = _slicedBuffer.asInputStream())
        {
            ByteStreams.copy(is, destination);
        }

        final byte[] expected =  new byte[source.length - 2];
        System.arraycopy(source, 1, expected, 0, expected.length);
        assertArrayEquals(expected, destination.toByteArray(), "Unexpected view result");
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

    private void testPutGet(final Class<?> primitiveTargetClass, final boolean unsigned, final Object value)
            throws Exception
    {
        final int size = sizeof(primitiveTargetClass);

        _parent.position(1);
        _parent.limit(size + 1);

        _slicedBuffer = _parent.slice();
        _parent.limit(_parent.capacity());

        assertEquals(0, (long) _slicedBuffer.position(), "Unexpected position ");
        assertEquals(size, (long) _slicedBuffer.limit(), "Unexpected limit ");
        assertEquals(size, (long) _slicedBuffer.capacity(), "Unexpected capacity ");

        final String methodSuffix = getMethodSuffix(primitiveTargetClass, unsigned);
        final Method put = _slicedBuffer.getClass().getMethod("put" + methodSuffix, Primitives.primitiveTypeOf(value.getClass()));
        final Method get = _slicedBuffer.getClass().getMethod("get" + methodSuffix);

        _slicedBuffer.mark();
        final QpidByteBuffer rv = (QpidByteBuffer) put.invoke(_slicedBuffer, value);
        assertEquals(_slicedBuffer, rv, "Unexpected builder return value for type " + methodSuffix);

        assertEquals(size, (long) _slicedBuffer.position(), "Unexpected position for type " + methodSuffix);

        assertThrows(BufferOverflowException.class, () -> invokeMethod(put, value),
                     "BufferOverflowException should be thrown for put with insufficient room for " + methodSuffix);

        _slicedBuffer.reset();

        assertEquals(0, (long) _slicedBuffer.position(), "Unexpected position after reset");

        Object retrievedValue = get.invoke(_slicedBuffer);
        assertEquals(value, retrievedValue, "Unexpected value retrieved from get method for " + methodSuffix);
        assertThrows(BufferUnderflowException.class, () -> invokeMethod(get),
                     "BufferUnderflowException not thrown for get with insufficient room for " + methodSuffix);

        _slicedBuffer.dispose();
        _slicedBuffer = null;
    }

    private void testPutGetByIndex(final Class<?> primitiveTargetClass, Object value) throws Exception
    {
        final int size = sizeof(primitiveTargetClass);

        _parent.position(1);
        _parent.limit(size + 1);

        _slicedBuffer = _parent.slice();
        _parent.limit(_parent.capacity());

        final String methodSuffix = getMethodSuffix(primitiveTargetClass, false);
        final Method put = _slicedBuffer.getClass().getMethod("put" + methodSuffix, int.class, primitiveTargetClass);
        final Method get = _slicedBuffer.getClass().getMethod("get" + methodSuffix, int.class);

        final QpidByteBuffer rv = (QpidByteBuffer) put.invoke(_slicedBuffer, 0, value);
        assertEquals(_slicedBuffer, rv, "Unexpected builder return value for type " + methodSuffix);

        final Object retrievedValue = get.invoke(_slicedBuffer, 0);
        assertEquals(value, retrievedValue,
                "Unexpected value retrieved from index get method for " + methodSuffix);
        assertThrows(IndexOutOfBoundsException.class, () -> invokeMethod(put, 1, value),
                "IndexOutOfBoundsException not thrown for  indexed " + methodSuffix + " put");
        assertThrows(IndexOutOfBoundsException.class, () -> invokeMethod(put, -1, value),
                "IndexOutOfBoundsException not thrown for indexed " + methodSuffix + " put with negative index");
        assertThrows(IndexOutOfBoundsException.class, () -> invokeMethod(get, 1),
                "IndexOutOfBoundsException not thrown for indexed " + methodSuffix + " get");
        assertThrows(IndexOutOfBoundsException.class, () -> invokeMethod(get, -1),
                "IndexOutOfBoundsException not thrown for indexed " + methodSuffix + " get with negative index");

        _slicedBuffer.dispose();
        _slicedBuffer = null;
    }

    private void invokeMethod(final Method method, final Object... value) throws Exception
    {
        try
        {
            method.invoke(_slicedBuffer, value);
        }
        catch (InvocationTargetException e)
        {
            final Throwable cause = e.getCause();
            if (cause instanceof Exception)
            {
                throw (Exception)cause;
            }
            fail(String.format("Unexpected throwable on method %s invocation: %s", method.getName(), cause));
        }
    }

    private String getMethodSuffix(final Class<?> target, final boolean unsigned)
    {
        final StringBuilder name = new StringBuilder();
        if (unsigned)
        {
            name.append("Unsigned");
        }
        if ((!target.isAssignableFrom(byte.class) || unsigned))
        {
            final String simpleName = target.getSimpleName();
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
    public void testPooledBufferIsZeroedLoan()
    {
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(BUFFER_SIZE))
        {
            buffer.put((byte) 0xFF);
        }

        try (final QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(BUFFER_SIZE))
        {
            buffer.limit(1);
            assertEquals((byte) 0x0, (long) buffer.get(), "Pooled QpidByteBuffer is not zeroed.");
        }
    }

    @Test
    public void testAllocateDirectOfSameSize()
    {
        final int bufferSize = BUFFER_SIZE;
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(bufferSize))
        {
            assertEquals(bufferSize, (long) buffer.capacity(), "Unexpected buffer size");
            assertEquals(0, (long) buffer.position(), "Unexpected position on newly created buffer");
            assertEquals(bufferSize, (long) buffer.limit(), "Unexpected limit on newly created buffer");
        }
    }

    @Test
    public void testAllocateDirectOfSmallerSize()
    {
        final int bufferSize = BUFFER_SIZE - 1;
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(bufferSize))
        {
            assertEquals(bufferSize, (long) buffer.capacity(), "Unexpected buffer size");
            assertEquals(0, (long) buffer.position(), "Unexpected position on newly created buffer");
            assertEquals(bufferSize, (long) buffer.limit(), "Unexpected limit on newly created buffer");
        }
    }

    @Test
    public void testAllocateDirectOfLargerSize()
    {
        final int bufferSize = BUFFER_SIZE + 1;
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(bufferSize))
        {
            assertEquals(bufferSize, (long) buffer.capacity(), "Unexpected buffer size");
            assertEquals(0, (long) buffer.position(), "Unexpected position on newly created buffer");
            assertEquals(bufferSize, (long) buffer.limit(), "Unexpected limit on newly created buffer");
        }
    }

    @Test
    public void testAllocateDirectWithNegativeSize()
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
    public void testSettingUpPoolTwice()
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
        final byte[] input = "aaabbbcccddddeeeffff".getBytes();
        try (final QpidByteBuffer inputBuf = QpidByteBuffer.allocateDirect(input.length))
        {
            inputBuf.put(input);
            inputBuf.flip();

            assertEquals(input.length, (long) inputBuf.remaining());

            doDeflateInflate(input, inputBuf, true);
        }
    }

    @Test
    public void testDeflateInflateHeap() throws Exception
    {
        final byte[] input = "aaabbbcccddddeeeffff".getBytes();
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(input))
        {
            doDeflateInflate(input, buffer, false);
        }
    }

    @Test
    public void testInflatingUncompressedBytes_ThrowsZipException() throws Exception
    {
        final byte[] input = "not_a_compressed_stream".getBytes();
        try (final QpidByteBuffer original = QpidByteBuffer.wrap(input))
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
    public void testSlice()
    {
        try (final QpidByteBuffer directBuffer = QpidByteBuffer.allocate(true, 6))
        {
            directBuffer.position(2);
            directBuffer.limit(5);
            try (final QpidByteBuffer directSlice = directBuffer.slice())
            {
                assertTrue(directSlice.isDirect(), "Direct slice should be direct too");
                assertEquals(3, (long) directSlice.capacity(), "Unexpected capacity");
                assertEquals(3, (long) directSlice.limit(), "Unexpected limit");
                assertEquals(0, (long) directSlice.position(), "Unexpected position");
            }
        }
    }

    @Test
    public void testView()
    {
        final byte[] content = "ABCDEF".getBytes();
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(true, content.length))
        {
            buffer.put(content);
            buffer.position(2);
            buffer.limit(5);

            try (final QpidByteBuffer view = buffer.view(0, buffer.remaining()))
            {
                assertTrue(view.isDirect(), "Unexpected view direct");
                assertEquals(3, (long) view.capacity(), "Unexpected capacity");
                assertEquals(3, (long) view.limit(), "Unexpected limit");
                assertEquals(0, (long) view.position(), "Unexpected position");

                final byte[] destination = new byte[view.remaining()];
                view.get(destination);
                assertArrayEquals("CDE".getBytes(), destination);
            }

            try (final QpidByteBuffer viewWithOffset = buffer.view(1, 1))
            {
                final byte[] destination = new byte[viewWithOffset.remaining()];
                viewWithOffset.get(destination);
                assertArrayEquals("D".getBytes(), destination);
            }
        }
    }

    @Test
    public void testSparsity()
    {
        assertFalse(_parent.isSparse(), "Unexpected sparsity after creation");
        final QpidByteBuffer child = _parent.view(0, 8);
        final QpidByteBuffer grandChild = child.view(0, 2);

        assertFalse(_parent.isSparse(), "Unexpected sparsity after child creation");
        _parent.dispose();
        _parent = null;

        assertFalse(child.isSparse(), "Unexpected sparsity after parent disposal");

        child.dispose();
        assertTrue(grandChild.isSparse(), "Buffer should be sparse");
        grandChild.dispose();
    }

    @Test
    public void testAsQpidByteBuffers() throws IOException
    {
        final byte[] dataForTwoBufs = "01234567890".getBytes(StandardCharsets.US_ASCII);
        try (final QpidByteBuffer qpidByteBuffer = QpidByteBuffer.asQpidByteBuffer(new ByteArrayInputStream(dataForTwoBufs)))
        {
            assertEquals(11, (long) qpidByteBuffer.remaining(), "Unexpected remaining in buf");
        }

        try (final QpidByteBuffer bufsForEmptyBytes = QpidByteBuffer.asQpidByteBuffer(new ByteArrayInputStream(new byte[]{})))
        {
            assertEquals(0, (long) bufsForEmptyBytes.remaining(), "Unexpected remaining in buf for empty buffer");
        }
    }

    @Test
    public void testConcatenate()
    {
        try (final QpidByteBuffer buffer1 = QpidByteBuffer.allocateDirect(10);
             final QpidByteBuffer buffer2 = QpidByteBuffer.allocateDirect(10))
        {
            final short buffer1Value = (short) (1 << 15);
            buffer1.putShort(buffer1Value);
            buffer1.flip();
            final char buffer2Value = 'x';
            buffer2.putChar(2, buffer2Value);
            buffer2.position(4);
            buffer2.flip();

            try (final QpidByteBuffer concatenate = QpidByteBuffer.concatenate(buffer1, buffer2))
            {
                assertEquals(6, (long) concatenate.capacity(), "Unexpected capacity");
                assertEquals(0, (long) concatenate.position(), "Unexpected position");
                assertEquals(6, (long) concatenate.limit(), "Unexpected limit");
                assertEquals(buffer1Value, (long) concatenate.getShort(), "Unexpected value 1");
                assertEquals(buffer2Value, (long) concatenate.getChar(4), "Unexpected value 2");
            }
        }
    }

    private void doDeflateInflate(final byte[] input,
                                  final QpidByteBuffer inputBuf,
                                  final boolean direct) throws IOException
    {
        try (final QpidByteBuffer deflatedBuf = QpidByteBuffer.deflate(inputBuf))
        {
            assertNotNull(deflatedBuf);

            try (final QpidByteBuffer inflatedBuf = QpidByteBuffer.inflate(deflatedBuf))
            {
                assertNotNull(inflatedBuf);

                final int inflatedBytesCount = inflatedBuf.remaining();

                final byte[] inflatedBytes = new byte[inflatedBytesCount];
                inflatedBuf.get(inflatedBytes);
                final byte[] expectedBytes = Arrays.copyOfRange(input, 0, inflatedBytes.length);
                assertArrayEquals(expectedBytes, inflatedBytes, "Inflated buf has unexpected content");

                assertEquals(input.length, (long) inflatedBytesCount, "Unexpected number of inflated bytes");
            }
        }
    }
}
