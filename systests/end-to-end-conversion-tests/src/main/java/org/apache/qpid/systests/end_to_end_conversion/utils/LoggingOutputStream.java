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

package org.apache.qpid.systests.end_to_end_conversion.utils;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.event.Level;

/*
 * Original code by Jim Moore
 * See: https://www.mail-archive.com/user@slf4j.org/msg00673.html
 * Adapted for Qpid needs.
 */

/**
 * An OutputStream that flushes out to a Category.<p>
 * <p/>
 * Note that no data is written out to the Category until the stream is
 * flushed or closed.<p>
 * <p/>
 * Example:<pre>
 * // make sure everything sent to System.err is logged
 * System.setErr(new PrintStream(new
 * LoggingOutputStream(Logger.getRootCategory(),
 * Level.WARN), true));
 * <p/>
 * // make sure everything sent to System.out is also logged
 * System.setOut(new PrintStream(new
 * LoggingOutputStream(Logger.getRootCategory(),
 * Level.INFO), true));
 * </pre>
 *
 * @author <a href="[EMAIL PROTECTED]">Jim Moore</a>
 */

//
public class LoggingOutputStream extends OutputStream
{
    /**
     * Platform dependant line separator
     */
    private static final byte[] LINE_SEPARATOR_BYTES = System.getProperty("line.separator").getBytes();
    /**
     * The default number of bytes in the buffer. =2048
     */
    private static final int DEFAULT_BUFFER_LENGTH = 2048;
    /**
     * Used to maintain the contract of [EMAIL PROTECTED] #close()}.
     */
    private boolean hasBeenClosed = false;
    /**
     * The internal buffer where data is stored.
     */
    private byte[] buf;
    /**
     * The number of valid bytes in the buffer. This value is always
     * in the range <tt>0</tt> through <tt>buf.length</tt>; elements
     * <tt>buf[0]</tt> through <tt>buf[count-1]</tt> contain valid
     * byte data.
     */
    private int count;
    /**
     * Remembers the size of the buffer for speed.
     */
    private int bufLength;
    /**
     * The category to write to.
     */
    private Logger logger;

    /**
     * The priority to use when writing to the Category.
     */
    private Level level;

    /**
     * Creates the LoggingOutputStream to flush to the given Category.
     *
     * @param log   the Logger to write to
     * @param level the Level to use when writing to the Logger
     * @throws IllegalArgumentException if cat == null or priority ==
     *                                  null
     */
    public LoggingOutputStream(Logger log, Level level) throws IllegalArgumentException
    {
        if (log == null)
        {
            throw new IllegalArgumentException("cat == null");
        }
        if (level == null)
        {
            throw new IllegalArgumentException("priority == null");
        }

        this.level = level;

        logger = log;
        bufLength = DEFAULT_BUFFER_LENGTH;
        buf = new byte[DEFAULT_BUFFER_LENGTH];
        count = 0;
    }


    /**
     * Closes this output stream and releases any system resources
     * associated with this stream. The general contract of
     * <code>close</code>
     * is that it closes the output stream. A closed stream cannot
     * perform
     * output operations and cannot be reopened.
     */
    public void close()
    {
        flush();
        hasBeenClosed = true;
    }


    /**
     * Writes the specified byte to this output stream. The general
     * contract for <code>write</code> is that one byte is written
     * to the output stream. The byte to be written is the eight
     * low-order bits of the argument <code>b</code>. The 24
     * high-order bits of <code>b</code> are ignored.
     *
     * @param b the <code>byte</code> to write
     * @throws java.io.IOException if an I/O error occurs. In particular, an
     *                             <code>IOException</code> may be
     *                             thrown if the output stream has been closed.
     */
    public void write(final int b) throws IOException
    {
        if (hasBeenClosed)
        {
            throw new IOException("The stream has been closed.");
        }

        // would this be writing past the buffer?

        if (count == bufLength)
        {
            // grow the buffer
            final int newBufLength = bufLength + DEFAULT_BUFFER_LENGTH;
            final byte[] newBuf = new byte[newBufLength];

            System.arraycopy(buf, 0, newBuf, 0, bufLength);
            buf = newBuf;

            bufLength = newBufLength;
        }

        buf[count] = (byte) b;

        count++;

        if (endsWithNewLine())
        {
            flush();
        }
    }

    private boolean endsWithNewLine()
    {
        if (count >= LINE_SEPARATOR_BYTES.length)
        {
            for (int i = 0; i < LINE_SEPARATOR_BYTES.length; i++)
            {
                if (buf[count - LINE_SEPARATOR_BYTES.length + i] != LINE_SEPARATOR_BYTES[i])
                {
                    return false;
                }
            }
            return true;
        }
        else
        {
            return false;
        }
    }


    /**
     * Flushes this output stream and forces any buffered output bytes
     * to be written out. The general contract of <code>flush</code> is
     * that calling it is an indication that, if any bytes previously
     * written have been buffered by the implementation of the output
     * stream, such bytes should immediately be written to their
     * intended destination.
     */
    public void flush()
    {

        if (count == 0)
        {
            return;
        }

        // don't print out blank lines; flushing from PrintStream puts

        // For linux system

        if (count == 1 && ((char) buf[0]) == '\n')
        {
            reset();
            return;
        }

        // For mac system

        if (count == 1 && ((char) buf[0]) == '\r')
        {
            reset();
            return;
        }

        // On windows system

        if (count == 2 && (char) buf[0] == '\r' && (char) buf[1] == '\n')
        {
            reset();
            return;
        }

        while (endsWithNewLine())
        {
            count -= LINE_SEPARATOR_BYTES.length;
        }
        final byte[] theBytes = new byte[count];
        System.arraycopy(buf, 0, theBytes, 0, count);
        final String message = new String(theBytes);
        switch (level)
        {
            case ERROR:
                logger.error(message);
                break;
            case WARN:
                logger.warn(message);
                break;
            case INFO:
                logger.info(message);
                break;
            case DEBUG:
                logger.debug(message);
                break;
            case TRACE:
                logger.trace(message);
                break;
        }
        reset();
    }

    private void reset()
    {
        // not resetting the buffer -- assuming that if it grew then it will likely grow similarly again
        count = 0;
    }
}
