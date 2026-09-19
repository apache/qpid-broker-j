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
package org.apache.qpid.server.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serial;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.ContextProvider;
import org.apache.qpid.server.model.NamedAddressSpace;

public class GZIPUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GZIPUtils.class);

    public static final String GZIP_CONTENT_ENCODING = "gzip";


    /**
     * Return a new byte array with the compressed contents of the input buffer
     *
     * @param input byte buffer to compress
     * @return a byte array containing the compressed data, or null if the input was null or there was an unexpected
     * IOException while compressing
     */
    public static byte[] compressBufferToArray(final ByteBuffer input)
    {
        if (input != null)
        {
            try (final ByteArrayOutputStream compressedBuffer = new ByteArrayOutputStream())
            {
                try (final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(compressedBuffer))
                {
                    if (input.hasArray())
                    {
                        gzipOutputStream.write(input.array(),
                                               input.arrayOffset() + input.position(),
                                               input.remaining());
                    }
                    else
                    {

                        final byte[] data = new byte[input.remaining()];

                        input.duplicate().get(data);

                        gzipOutputStream.write(data);
                    }
                }
                return compressedBuffer.toByteArray();
            }
            catch (final IOException e)
            {
                LOGGER.warn("Unexpected IOException when attempting to compress with gzip", e);
            }
        }
        return null;
    }

    /**
     * @deprecated use {@link #uncompressBufferToArray(ByteBuffer, int)} to provide an explicit limit
     */
    @Deprecated
    public static byte[] uncompressBufferToArray(final ByteBuffer contentBuffer)
    {
        try
        {
            return uncompressBufferToArray(contentBuffer, Connection.DEFAULT_MAX_MESSAGE_DECOMPRESSION_SIZE);
        }
        catch (final GZIPInflationLimitException ignore)
        {
            // Preserve the legacy null-on-failure contract.
            return null;
        }
    }

    public static byte[] uncompressBufferToArray(final ByteBuffer contentBuffer, final int maximumOutputSize)
            throws GZIPInflationLimitException
    {
        validateMaximumOutputSize(maximumOutputSize);
        if (contentBuffer != null)
        {
            try (final ByteBufferInputStream input = new ByteBufferInputStream(contentBuffer))
            {
                return uncompressStreamToArray(input, maximumOutputSize);
            }
        }
        else
        {
            return null;
        }
    }

    /**
     * @deprecated use {@link #uncompressStreamToArray(InputStream, int)} to provide an explicit limit
     */
    @Deprecated
    public static byte[] uncompressStreamToArray(final InputStream stream)
    {
        try
        {
            return uncompressStreamToArray(stream, Connection.DEFAULT_MAX_MESSAGE_DECOMPRESSION_SIZE);
        }
        catch (final GZIPInflationLimitException ignore)
        {
            // Preserve the legacy null-on-failure contract.
            return null;
        }
    }

    public static byte[] uncompressStreamToArray(final InputStream stream, final int maximumOutputSize)
            throws GZIPInflationLimitException
    {
        validateMaximumOutputSize(maximumOutputSize);
        if (stream != null)
        {
            try (final GZIPInputStream gzipInputStream = new GZIPInputStream(stream))
            {
                final ByteArrayOutputStream inflatedContent = new ByteArrayOutputStream();
                final byte[] buf = new byte[4096];
                int uncompressedSize = 0;
                int read;
                while ((read = gzipInputStream.read(buf)) != -1)
                {
                    if (read > maximumOutputSize - uncompressedSize)
                    {
                        throw new GZIPInflationLimitException(String.format("Decompressed content exceeds the maximum " +
                                "size of %d bytes", maximumOutputSize));
                    }
                    uncompressedSize += read;
                    inflatedContent.write(buf, 0, read);
                }
                return inflatedContent.toByteArray();
            }
            catch (final GZIPInflationLimitException e)
            {
                throw e;
            }
            catch (final IOException e)
            {
                LOGGER.warn("Unexpected IOException when attempting to uncompress with gzip", e);
            }
        }
        return null;
    }

    public static void validateDecompressedSize(final InputStream stream, final int maximumOutputSize)
            throws GZIPInflationLimitException
    {
        validateMaximumOutputSize(maximumOutputSize);
        if (stream != null)
        {
            try (final GZIPInputStream gzipInputStream = new GZIPInputStream(stream))
            {
                final byte[] buffer = new byte[4096];
                int uncompressedSize = 0;
                int read;
                while ((read = gzipInputStream.read(buffer)) != -1)
                {
                    if (read > maximumOutputSize - uncompressedSize)
                    {
                        throw new GZIPInflationLimitException(String.format("Decompressed content exceeds the " +
                                "maximum size of %d bytes", maximumOutputSize));
                    }
                    uncompressedSize += read;
                }
            }
            catch (final GZIPInflationLimitException e)
            {
                throw e;
            }
            catch (final IOException e)
            {
                LOGGER.warn("Unexpected IOException when attempting to validate gzip content", e);
            }
        }
    }

    public static int getMaximumMessageDecompressionSize(final ContextProvider contextProvider)
    {
        return getMaximumMessageDecompressionSize(contextProvider, Integer.MAX_VALUE);
    }

    public static int getMaximumMessageDecompressionSizeForAddressSpace(final NamedAddressSpace addressSpace)
    {
        final ContextProvider contextProvider = addressSpace instanceof ContextProvider
                ? (ContextProvider) addressSpace
                : null;
        return getMaximumMessageDecompressionSize(contextProvider);
    }

    public static int getMaximumMessageDecompressionSize(final ContextProvider contextProvider,
                                                          final long maximumMessageSize)
    {
        final int configuredLimit = getPositiveContextValue(contextProvider,
                Connection.MAX_MESSAGE_DECOMPRESSION_SIZE, Connection.DEFAULT_MAX_MESSAGE_DECOMPRESSION_SIZE);
        final int messageSizeLimit = getPositiveContextValue(contextProvider, Connection.MAX_MESSAGE_SIZE,
                Integer.MAX_VALUE);
        final long connectionLimit = maximumMessageSize > 0 ? maximumMessageSize : Integer.MAX_VALUE;
        final long effectiveLimit = Math.min(Math.min(configuredLimit, messageSizeLimit), connectionLimit);
        return (int) Math.min(effectiveLimit, Integer.MAX_VALUE);
    }

    private static int getPositiveContextValue(final ContextProvider contextProvider,
                                               final String contextName,
                                               final int defaultValue)
    {
        if (contextProvider != null)
        {
            try
            {
                final Integer value = contextProvider.getContextValue(Integer.class, contextName);
                if (value != null && value > 0)
                {
                    return value;
                }
            }
            catch (final NullPointerException | IllegalArgumentException e)
            {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    private static void validateMaximumOutputSize(final int maximumOutputSize)
    {
        if (maximumOutputSize < 0)
        {
            throw new IllegalArgumentException("maximumOutputSize cannot be negative");
        }
    }

    public static final class GZIPInflationLimitException extends IOException
    {
        @Serial
        private static final long serialVersionUID = 1L;

        public GZIPInflationLimitException()
        {
            super();
        }

        public GZIPInflationLimitException(final String message)
        {
            super(message);
        }

        public GZIPInflationLimitException(final String message, final Throwable cause)
        {
            super(message, cause);
        }
    }
}
