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

package org.apache.qpid.server.management.plugin;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

public class GunzipOutputStream extends InflaterOutputStream
{
    private final GZIPHeader _header = new GZIPHeader();
    private final GZIPTrailer _trailer = new GZIPTrailer();
    private final byte[] _singleByteArray = new byte[1];
    private final CRC32 _crc;
    private StreamState _streamState = StreamState.HEADER_PARSING;

    public GunzipOutputStream(final OutputStream targetOutputStream)
    {
        super(new CheckedOutputStream(targetOutputStream, new CRC32()), new Inflater(true));
        _crc = (CRC32)((CheckedOutputStream)out).getChecksum();
    }

    @Override
    public void write(final byte data[], final int offset, final int length) throws IOException
    {
        try(ByteArrayInputStream bais = new ByteArrayInputStream(data, offset, length))
        {
            int b;
            while ((b = bais.read()) != -1)
            {
                if (_streamState == StreamState.DONE)
                {
                    // another member is coming
                    _streamState = StreamState.HEADER_PARSING;
                    _crc.reset();
                    _header.reset();
                    _trailer.reset();
                    inf.reset();
                }

                if (_streamState == StreamState.HEADER_PARSING)
                {
                    _header.headerByte(b);
                    if (_header.getState() == HeaderState.DONE)
                    {
                        _streamState = StreamState.INFLATING;
                        continue;
                    }
                }

                if (_streamState == StreamState.INFLATING)
                {
                    _singleByteArray[0] = (byte) b;
                    super.write(_singleByteArray, 0, 1);

                    if (inf.finished())
                    {
                        _streamState = StreamState.TRAILER_PARSING;
                        continue;
                    }
                }

                if (_streamState == StreamState.TRAILER_PARSING)
                {
                    if (_trailer.trailerByte(b))
                    {
                        _trailer.verify(_crc);
                        _streamState = StreamState.DONE;
                        continue;
                    }
                }
            }
        }
    }

    private enum StreamState
    {
        HEADER_PARSING, INFLATING, TRAILER_PARSING, DONE
    }

    private enum HeaderState
    {
        ID1, ID2, CM, FLG, MTIME_0, MTIME_1, MTIME_2, MTIME_3, XFL, OS, XLEN_0, XLEN_1, FEXTRA, FNAME, FCOMMENT, CRC16_0, CRC16_1, DONE
    }

    private class GZIPHeader
    {
        private static final int GZIP_MAGIC_1 = 0x1F;
        private static final int GZIP_MAGIC_2 = 0x8B;
        private static final int SUPPORTED_COMPRESSION_METHOD = Deflater.DEFLATED;
        private HeaderState _state = HeaderState.ID1;
        private byte _flags;
        private byte _xlen0;
        private int _xlen;
        private int _fExtraCounter;

        private void headerByte(int headerByte) throws IOException
        {
            int b = headerByte & 0xff;
            switch (_state)
            {
                case ID1:
                    if (b != GZIP_MAGIC_1)
                    {
                        throw new IOException(String.format("Incorrect first magic byte: got '%X' but expected '%X'",
                                                            headerByte,
                                                            GZIP_MAGIC_1));
                    }
                    _state = HeaderState.ID2;
                    break;
                case ID2:
                    if (b != GZIP_MAGIC_2)
                    {
                        throw new IOException(String.format("Incorrect second magic byte: got '%X' but expected '%X'",
                                                            headerByte,
                                                            GZIP_MAGIC_2));
                    }
                    _state = HeaderState.CM;
                    break;
                case CM:
                    if (b != SUPPORTED_COMPRESSION_METHOD)
                    {
                        throw new IOException(String.format("Unexpected compression method : '%X'", b));
                    }
                    _state = HeaderState.FLG;
                    break;
                case FLG:
                    _flags = (byte) b;
                    _state = HeaderState.MTIME_0;
                    break;
                case MTIME_0:
                    _state = HeaderState.MTIME_1;
                    break;
                case MTIME_1:
                    _state = HeaderState.MTIME_2;
                    break;
                case MTIME_2:
                    _state = HeaderState.MTIME_3;
                    break;
                case MTIME_3:
                    _state = HeaderState.XFL;
                    break;
                case XFL:
                    _state = HeaderState.OS;
                    break;
                case OS:
                    adjustStateAccordingToFlags();
                    break;
                case XLEN_0:
                    _xlen0 = (byte) b;
                    _state = HeaderState.XLEN_1;
                    break;
                case XLEN_1:
                    _xlen = b << 8 | _xlen0;
                    _state = HeaderState.FEXTRA;
                    break;
                case FEXTRA:
                    _fExtraCounter++;
                    if (_fExtraCounter == _xlen)
                    {
                        adjustStateAccordingToFlags(HeaderState.XLEN_0);
                    }
                    break;
                case FNAME:
                    if (b == 0)
                    {
                        adjustStateAccordingToFlags(HeaderState.XLEN_0, HeaderState.FNAME);
                    }
                    break;
                case FCOMMENT:
                    if (b == 0)
                    {
                        adjustStateAccordingToFlags(HeaderState.XLEN_0, HeaderState.FNAME, HeaderState.FCOMMENT);
                    }
                    break;
                case CRC16_0:
                    _state = HeaderState.CRC16_1;
                    break;
                case CRC16_1:
                    _state = HeaderState.DONE;
                    break;
                default:
                    throw new IOException("Unexpected state " + _state);
            }
        }

        private void adjustStateAccordingToFlags(HeaderState... previousStates)
        {
            EnumSet<HeaderState> previous = previousStates.length == 0
                    ? EnumSet.noneOf(HeaderState.class)
                    : EnumSet.copyOf(Arrays.asList(previousStates));
            if ((_flags & (byte) 4) != 0 && !previous.contains(HeaderState.XLEN_0))
            {
                _state = HeaderState.XLEN_0;
            }
            else if ((_flags & (byte) 8) != 0 && !previous.contains(HeaderState.FNAME))
            {
                _state = HeaderState.FNAME;
            }
            else if ((_flags & (byte) 16) != 0 && !previous.contains(HeaderState.FCOMMENT))
            {
                _state = HeaderState.FCOMMENT;
            }
            else if ((_flags & (byte) 2) != 0 && !previous.contains(HeaderState.CRC16_0))
            {
                _state = HeaderState.CRC16_0;
            }
            else
            {
                _state = HeaderState.DONE;
            }
        }

        private HeaderState getState()
        {
            return _state;
        }

        private void reset()
        {
            _state = HeaderState.ID1;
            _flags = 0;
            _xlen0 = 0;
            _xlen = 0;
            _fExtraCounter = 0;
        }
    }

    private class GZIPTrailer
    {
        private static final int TRAILER_SIZE = 8;
        private static final long SIZE_MASK = 0xffffffffL;
        private byte[] _trailerBytes = new byte[TRAILER_SIZE];
        private int _receivedByteIndex;

        private boolean trailerByte(int b) throws IOException
        {
            if (_receivedByteIndex < TRAILER_SIZE)
            {
                _trailerBytes[_receivedByteIndex++] = (byte) (b & 0xff);
                return _receivedByteIndex == TRAILER_SIZE;
            }
            else
            {
                throw new IOException(
                        String.format("Received too many GZIP trailer bytes. Expecting %d bytes.", TRAILER_SIZE));
            }
        }

        private void verify(CRC32 crc) throws IOException
        {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(_trailerBytes)))
            {
                long crc32 = readLittleEndianLong(in);
                if (crc32 != crc.getValue())
                {
                    throw new IOException("crc32 mismatch. Gzip-compressed data is corrupted");
                }
                long isize = readLittleEndianLong(in);
                if (isize != (inf.getBytesWritten() & SIZE_MASK))
                {
                    throw new IOException("Uncompressed size mismatch. Gzip-compressed data is corrupted");
                }
            }
        }

        private long readLittleEndianLong(final DataInputStream inData) throws IOException
        {
            return inData.readUnsignedByte()
                   | (inData.readUnsignedByte() << 8)
                   | (inData.readUnsignedByte() << 16)
                   | (((long) inData.readUnsignedByte()) << 24);
        }

        private void reset()
        {
            _receivedByteIndex = 0;
        }

    }
}
