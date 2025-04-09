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

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.security.SecureRandom;
import java.util.stream.Collectors;

public class StringUtil
{
    private static final String NUMBERS = "0123456789";
    private static final String LETTERS = "abcdefghijklmnopqrstuvwxwy";
    private static final String OTHERS = "_-";
    private static final char[] CHARACTERS = (NUMBERS + LETTERS + LETTERS.toUpperCase() + OTHERS).toCharArray();
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();
    private static final Map<CharSequence, CharSequence> HTML_ESCAPE = Map.of("\"", "&quot;",
            "&", "&amp;",
            "<", "&lt;",
            ">", "&gt;",
            "'", "&#x27;");
    private static final BitSet HTML_ESCAPE_PREFIX_SET = new BitSet();
    private static final int LONGEST_HTML_ESCAPE_ENTRY = HTML_ESCAPE.values().stream()
            .max(Comparator.comparingInt(CharSequence::length))
            .get().length();
    private static final Random RANDOM = new SecureRandom();

    static
    {
        HTML_ESCAPE.keySet().forEach(key -> HTML_ESCAPE_PREFIX_SET.set(key.charAt(0)));

    }

    public static String elideDataUrl(final String path)
    {
        return String.valueOf(path).toLowerCase().startsWith("data:") ? "data:..." : path;
    }

    public static String toHex(byte[] bin)
    {
        StringBuilder result = new StringBuilder(2 * bin.length);
        for (byte b : bin) {
            result.append(HEX[(b >> 4) & 0xF]);
            result.append(HEX[(b & 0xF)]);
        }
        return result.toString();
    }

    public String randomAlphaNumericString(int maxLength)
    {
        char[] result = new char[maxLength];
        for (int i = 0; i < maxLength; i++)
        {
            result[i] = CHARACTERS[RANDOM.nextInt(CHARACTERS.length)];
        }
        return new String(result);
    }

    /**
     * Builds a legal java name, based on manager name if possible,
     * this is unique for the given input.
     *
     * @param managerName
     * @return unique java name
     */
    public String createUniqueJavaName(final String managerName)
    {
        final StringBuilder builder = new StringBuilder();
        boolean initialChar = true;
        for (int i = 0; i < managerName.length(); i++)
        {
            char c = managerName.charAt(i);
            if ((initialChar && Character.isJavaIdentifierStart(c))
                    || (!initialChar && Character.isJavaIdentifierPart(c)))
            {
                builder.append(c);
                initialChar = false;
            }
        }
        if (builder.length() > 0)
        {
            builder.append("_");
        }
        try
        {
            final byte[] digest = MessageDigest.getInstance("MD5").digest(managerName.getBytes(StandardCharsets.UTF_8));
            builder.append(toHex(digest).toLowerCase());
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new ServerScopedRuntimeException(e);
        }
        return builder.toString();
    }

    public static String escapeHtml4(final String input)
    {
        if (input == null)
        {
            return null;
        }
        try
        {
            final StringWriter writer = new StringWriter(input.length() * 2);
            translate(input, writer);
            return writer.toString();
        }
        catch (IOException e)
        {
            throw new ConnectionScopedRuntimeException(e);
        }
    }

    private static void translate(final CharSequence input, final Writer writer) throws IOException
    {
        int pos = 0;
        int len = input.length();

        while (pos < len)
        {
            int consumed = translate(input, pos, writer);
            if (consumed == 0)
            {
                char c1 = input.charAt(pos);
                writer.write(c1);
                ++pos;
                if (Character.isHighSurrogate(c1) && pos < len)
                {
                    char c2 = input.charAt(pos);
                    if (Character.isLowSurrogate(c2))
                    {
                        writer.write(c2);
                        ++pos;
                    }
                }
            }
            else
            {
                for (int pt = 0; pt < consumed; ++pt)
                {
                    pos += Character.charCount(Character.codePointAt(input, pos));
                }
            }
        }
    }

    private static int translate(final CharSequence input, final int index, final Writer writer) throws IOException
    {
        if (HTML_ESCAPE_PREFIX_SET.get(input.charAt(index)))
        {
            int max = LONGEST_HTML_ESCAPE_ENTRY;
            if (index + LONGEST_HTML_ESCAPE_ENTRY > input.length())
            {
                max = input.length() - index;
            }

            for (int i = max; i >= 1; --i)
            {
                final CharSequence subSeq = input.subSequence(index, index + i);
                final String result = (String) HTML_ESCAPE.get(subSeq.toString());
                if (result != null)
                {
                    writer.write(result);
                    return Character.codePointCount(subSeq, 0, subSeq.length());
                }
            }
        }
        return 0;
    }

    public static String join(final Collection<?> collection, final String delimiter, final String useForNull)
    {
        if (collection == null)
        {
            return "";
        }
        return collection.stream()
                         .map(el -> el == null ? useForNull : String.valueOf(el))
                         .collect(Collectors.joining(delimiter));
    }

    public static String join(final Map<?, ?> map, final String delimiter, final String keyValueSeparator)
    {
        if (map == null)
        {
            return "";
        }
        return map.entrySet().stream()
                  .map(entry -> entry.getKey() + keyValueSeparator + entry.getValue())
                  .collect(Collectors.joining(delimiter));
    }
}
