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
package org.apache.qpid.server.management.plugin.csv;


import java.io.IOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;

/**
 * Simplified version of CSVFormat from Apache Commons CSV
 */
public final class CSVFormat
{
    private static final char COMMA = ',';

    private static final char COMMENT = '#';

    private static final char CR = '\r';

    private static final String CRLF = "\r\n";

    private static final Character DOUBLE_QUOTE_CHAR = '"';

    private static final String EMPTY = "";

    private static final char LF = '\n';

    private static final char SP = ' ';
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final char _delimiter;

    private final Character _quoteCharacter; // null if quoting is disabled

    private final String _recordSeparator; // for outputs

    public CSVFormat()
    {
        this(COMMA, DOUBLE_QUOTE_CHAR, CRLF);
    }

    /**
     * Creates a customized CSV format.
     *
     * @param delimiter       the char used for value separation, must not be a line break character
     * @param quoteCharacter  the Character used as value encapsulation marker, may be {@code null} to disable
     * @param recordSeparator the line separator to use for output
     * @throws IllegalArgumentException if the _delimiter is a line break character
     */
    private CSVFormat(final char delimiter,
              final Character quoteCharacter,
              final String recordSeparator)
    {
        if (delimiter == LF || delimiter == CR)
        {
            throw new IllegalArgumentException("The _delimiter cannot be a line break");
        }

        if (quoteCharacter != null && delimiter == quoteCharacter)
        {
            throw new IllegalArgumentException(
                    "The quote character and the delimiter cannot be the same ('" + quoteCharacter + "')");
        }

        this._delimiter = delimiter;
        this._quoteCharacter = quoteCharacter;
        this._recordSeparator = recordSeparator;
    }

    public <T extends Collection<?>> void printRecord(final Appendable out, final T record) throws IOException
    {
        boolean newRecord = true;
        for (Object item : record)
        {
            print(out, item, newRecord);
            newRecord = false;
        }
        println(out);
    }

    public <C extends Collection<? extends Collection<?>>> void printRecords(final Appendable out, final C records)
            throws IOException
    {
        for (Collection<?> record : records)
        {
            printRecord(out, record);
        }
    }


    public void println(final Appendable out) throws IOException
    {
        if (_recordSeparator != null)
        {
            out.append(_recordSeparator);
        }
    }

    public void print(final Appendable out, final Object value, final boolean newRecord) throws IOException
    {
        CharSequence charSequence;
        if (value == null)
        {
            charSequence = EMPTY;
        }
        else if (value instanceof CharSequence)
        {
            charSequence = (CharSequence) value;
        }
        else if (value instanceof Date || value instanceof Calendar)
        {
            final Date time = value instanceof Calendar ? ((Calendar) value).getTime() : (Date) value;

            // CSV standard (rfc4180) does not specify the date time format
            // Excel CSV format is local specific
            // Some posts on stackoverflow indicate that Excel should support format "yyyy-MM-dd HH:mm:ss".
            // for example, https://stackoverflow.com/questions/804118/best-timestamp-format-for-csv-excel
            // Perhaps, it would be better to convert datetime into long (similar to json datetime representation)
            charSequence = DATE_TIME_FORMATTER.format(time.toInstant().atZone(ZoneId.systemDefault()));
        }
        else
        {
            charSequence = value.toString();
        }
        this.print(out, value, charSequence, 0, charSequence.length(), newRecord);
    }


    public void printComments(final Appendable out,
                              final String... comments) throws IOException
    {
        for (String comment: comments)
        {
            out.append(COMMENT).append(SP).append(comment);
            println(out);
        }
    }

    private void print(final Appendable out,
                       final Object object,
                       final CharSequence value,
                       final int offset,
                       final int len,
                       final boolean newRecord) throws IOException
    {
        if (!newRecord)
        {
            out.append(_delimiter);
        }
        if (object == null)
        {
            out.append(value);
        }
        else if (_quoteCharacter != null)
        {
            printAndQuote(value, offset, len, out, newRecord);
        }
        else
        {
            out.append(value, offset, offset + len);
        }
    }

    private void printAndQuote(final CharSequence value, final int offset, final int len,
                               final Appendable out, final boolean newRecord) throws IOException
    {
        boolean quote = false;
        int start = offset;
        int pos = offset;
        final int end = offset + len;

        final char quoteChar = _quoteCharacter;

        if (len <= 0)
        {
            // always quote an empty token that is the first
            // on the line, as it may be the only thing on the
            // line. If it were not quoted in that case,
            // an empty line has no tokens.
            if (newRecord)
            {
                quote = true;
            }
        }
        else
        {
            char c = value.charAt(pos);

            if (c <= COMMENT)
            {
                // Some other chars at the start of a value caused the parser to fail, so for now
                // encapsulate if we start in anything less than '#'. We are being conservative
                // by including the default comment char too.
                quote = true;
            }
            else
            {
                while (pos < end)
                {
                    c = value.charAt(pos);
                    if (c == LF || c == CR || c == quoteChar || c == _delimiter)
                    {
                        quote = true;
                        break;
                    }
                    pos++;
                }

                if (!quote)
                {
                    pos = end - 1;
                    c = value.charAt(pos);
                    // Some other chars at the end caused the parser to fail, so for now
                    // encapsulate if we end in anything less than ' '
                    if (c <= SP)
                    {
                        quote = true;
                    }
                }
            }
        }

        if (!quote)
        {
            // no encapsulation needed - write out the original value
            out.append(value, start, end);
            return;
        }

        // we hit something that needed encapsulation
        out.append(quoteChar);

        // Pick up where we left off: pos should be positioned on the first character that caused
        // the need for encapsulation.
        while (pos < end)
        {
            final char c = value.charAt(pos);
            if (c == quoteChar)
            {
                // write out the chunk up until this point

                // add 1 to the length to write out the encapsulator also
                out.append(value, start, pos + 1);
                // put the next starting position on the encapsulator so we will
                // write it out again with the next string (effectively doubling it)
                start = pos;
            }
            pos++;
        }

        // write the last segment
        out.append(value, start, pos);
        out.append(quoteChar);
    }

}
