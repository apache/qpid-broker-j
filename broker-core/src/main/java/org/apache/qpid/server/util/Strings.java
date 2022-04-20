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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * String utilities
 */
public final class Strings
{
    /**
     * Utility class shouldn't be instantiated directly
     */
    private Strings()
    {

    }

    /**
     * Empty byte array
     */
    private static final byte[] EMPTY = new byte[0];

    /**
     * Thread bound character buffer
     */
    private static final ThreadLocal<char[]> CHAR_BUFFER = ThreadLocal.withInitial(() -> new char[4096]);

    /**
     * Variable regexp pattern
     */
    private static final Pattern VAR = Pattern.compile("\\$\\{([^}]*)}|\\$(\\$)");

    /**
     * Null resolver, always returning null
     */
    private static final Resolver NULL_RESOLVER = (variable, resolver) -> null;

    /**
     * Environment variable resolver
     */
    public static final Resolver ENV_VARS_RESOLVER = (variable, resolver) -> System.getenv(variable);

    /**
     * System property resolver
     */
    public static final Resolver JAVA_SYS_PROPS_RESOLVER = (variable, resolver) -> System.getProperty(variable);

    /**
     * System resolver chaining environment variable and system property resolvers
     */
    public static final Resolver SYSTEM_RESOLVER = chain(JAVA_SYS_PROPS_RESOLVER, ENV_VARS_RESOLVER);

    /**
     * Chains several resolvers into a ChainedResolver instance
     *
     * @param resolvers Resolvers to be chained
     *
     * @return Resulting ChainedResolver
     */
    public static Resolver chain(final Resolver... resolvers)
    {
        Resolver resolver;
        if(resolvers.length == 0)
        {
            resolver = NULL_RESOLVER;
        }
        else
        {
            resolver = resolvers[resolvers.length - 1];
            for (int i = resolvers.length - 2; i >= 0; i--)
            {
                resolver = new ChainedResolver(resolvers[i], resolver);
            }
        }
        return resolver;
    }

    /**
     * Converts string to the UTF8 encoded byte array
     *
     * @param str Source string
     *
     * @return Byte array
     */
    public static byte[] toUTF8(final String str)
    {
        if (str == null)
        {
            return EMPTY;
        }
        else
        {
            final int size = str.length();
            char[] chars = CHAR_BUFFER.get();
            if (size > chars.length)
            {
                chars = new char[Math.max(size, 2*chars.length)];
                CHAR_BUFFER.set(chars);
            }

            str.getChars(0, size, chars, 0);
            final byte[] bytes = new byte[size];
            for (int i = 0; i < size; i++)
            {
                if (chars[i] > 127)
                {
                    return str.getBytes(StandardCharsets.UTF_8);
                }
                bytes[i] = (byte) chars[i];
            }
            return bytes;
        }
    }

    /**
     * Decodes base64 encoded string into a byte array
     *
     * @param base64String Base64 encoded string
     * @param description String description provided for logging purposes
     *
     * @return Resulting byte array
     */
    public static byte[] decodePrivateBase64(final String base64String, final String description)
    {
        if (isInvalidBase64String(base64String))
        {
            // do not add base64String to exception message as it can contain private data
            throw new IllegalArgumentException("Cannot convert " + description +
                    " string to a byte[] - it does not appear to be base64 data");
        }
        return Base64.getDecoder().decode(base64String);
    }

    /**
     * Decodes base64 encoded char array into a byte array
     *
     * @param base64Chars Base64 encoded char array
     * @param description String description provided for logging purposes
     *
     * @return Resulting byte array
     */
    public static byte[] decodeCharArray(final char[] base64Chars, final String description)
    {
        if (base64Chars == null)
        {
            return null;
        }
        try
        {
            final CharBuffer charBuffer = CharBuffer.wrap(base64Chars);
            final ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(charBuffer);
            return Base64.getDecoder().decode(byteBuffer).array();
        }
        catch (IllegalArgumentException e)
        {
            // do not add base64String to exception message as it can contain private data
            throw new IllegalArgumentException("Cannot convert "
                                               + description
                                               + " string to a byte[] - it does not appear to be base64 data");
        }
    }

    /**
     * Fills byte arrays with blank characters
     *
     * @param bytes Byte arrays to be cleared
     */
    public static void clearByteArray(byte[]... bytes)
    {
        for (final byte[] array : bytes)
        {
            if (array != null)
            {
                Arrays.fill(array, (byte) 0);
            }
        }
    }

    /**
     * Converts an object to the ClearableCharSequence
     *
     * @param object Object to convert
     *
     * @return ClearableCharSequence instance
     */
    public static ClearableCharSequence toCharSequence(final Object object)
    {
        return new ClearableCharSequence(object);
    }

    /**
     * Decodes base64 encoded string into a byte array
     *
     * @param base64String Base64 encoded string
     *
     * @return Resulting byte array
     */
    public static byte[] decodeBase64(final String base64String)
    {
        if (isInvalidBase64String(base64String))
        {
            throw new IllegalArgumentException("Cannot convert string '" + base64String +
                    "' to a byte[] - it does not appear to be base64 data");
        }
        return Base64.getDecoder().decode(base64String);
    }

    /**
     * Checks if string is valid base64 encoded string or not
     *
     * @param base64String Base64 encoded string
     *
     * @return True when parameter passed is valid base64 encoded string, false otherwise
     */
    private static boolean isInvalidBase64String(final String base64String)
    {
        return !base64String.replaceAll("\\s", "").matches("^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$");
    }

    /**
     * Expands string replacing variables it contains with variable values
     *
     * @param input Source string
     * @param resolver Resolver to use
     *
     * @return Expanded string
     */
    public static String expand(final String input, final Resolver resolver)
    {
        return expand(input, resolver, new Stack<>(),true);
    }

    /**
     * Expands string replacing variables it contains with variable values
     *
     * @param input Source string
     * @param failOnUnresolved Boolean flag defining if an exception should be thrown in case of failed resolution
     * @param resolvers Resolvers to use
     *
     * @return Expanded string
     */
    public static String expand(final String input, final boolean failOnUnresolved, final Resolver... resolvers)
    {
        return expand(input, chain(resolvers), new Stack<>(), failOnUnresolved);
    }

    /**
     * Expands string replacing variables it contains with variable values
     *
     * @param input Source string
     * @param resolver Resolver
     * @param stack Stack containing variable chain
     * @param failOnUnresolved Boolean flag defining if an exception should be thrown in case of failed resolution
     *
     * @return Expanded string
     */
    private static String expand(
        final String input,
        final Resolver resolver,
        final Stack<String> stack,
        final boolean failOnUnresolved
    )
    {
        if (input == null)
        {
            return null;
        }
        final Matcher m = VAR.matcher(input);
        final StringBuffer result = new StringBuffer();
        while (m.find())
        {
            final String var = m.group(1);
            if (var == null)
            {
                final String esc = m.group(2);
                if ("$".equals(esc))
                {
                    m.appendReplacement(result, Matcher.quoteReplacement("$"));
                }
                else
                {
                    throw new IllegalArgumentException(esc);
                }
            }
            else
            {
                m.appendReplacement(result, Matcher.quoteReplacement(resolve(var, resolver, stack, failOnUnresolved)));
            }
        }
        m.appendTail(result);
        return result.toString();
    }

    /**
     * Resolves variable
     *
     * @param var Variable name
     * @param resolver Resolver
     * @param stack Stack containing variable chain
     * @param failOnUnresolved Boolean flag defining if an exception should be thrown in case of failed resolution
     *
     * @return Resolved variable value
     */
    private static String resolve(
        final String var,
        final Resolver resolver,
        final Stack<String> stack,
        final boolean failOnUnresolved
    )
    {
        if (stack.contains(var))
        {
            throw new IllegalArgumentException
                (String.format("recursively defined variable: %s stack=%s", var,
                               stack));
        }

        final String result = resolver.resolve(var, resolver);
        if (result == null)
        {
            if (failOnUnresolved)
            {
                throw new IllegalArgumentException("no such variable: " + var);
            }
            else
            {
                return "${"+var+"}";
            }
        }

        stack.push(var);
        try
        {
            return expand(result, resolver, stack, failOnUnresolved);
        }
        finally
        {
            stack.pop();
        }
    }

    /**
     * Joins string representation of an object iterable
     *
     * @param sep Separator
     * @param items Object iterable
     *
     * @return Resulting string
     */
    public static String join(final String sep, final Iterable<?> items)
    {
        Objects.requireNonNull(sep, "Separator must be not null");
        Objects.requireNonNull(items, "Items must be not null");
        final StringBuilder result = new StringBuilder();
        for (final Object object : items)
        {
            if (result.length() > 0)
            {
                result.append(sep);
            }
            result.append(object == null ? "null" : object.toString());
        }
        return result.toString();
    }

    /**
     * Joins string representation of an object array
     *
     * @param sep Separator
     * @param items Object array
     *
     * @return Resulting string
     */
    public static String join(final String sep, final Object[] items)
    {
        Objects.requireNonNull(items, "Items must be not null");
        return join(sep, Arrays.asList(items));
    }

    /**
     * Splits source string into a liast of tokens separated by comma
     *
     * @param listAsString Source string
     *
     * @return List of tokens
     */
    public static List<String> split(final String listAsString)
    {
        if (listAsString != null && !"".equals(listAsString))
        {
            return Arrays.asList(listAsString.split("\\s*,\\s*"));
        }
        return Collections.emptyList();
    }

    /**
     * Dumps bytes in the textual format used by UNIX od(1) in hex (x4) mode i.e. {@code od -Ax -tx1 -v}.
     *
     * This format is understood by Wireshark "Import from HexDump" feature so is useful for dumping buffers
     * containing AMQP 1.0 byte-streams for diagnostic purposes.
     *
     * @param buf - buffer to be dumped.  Buffer will be unchanged.
     */
    public static String hexDump(final ByteBuffer buf)
    {
        final StringBuilder builder = new StringBuilder();
        int count = 0;
        for (int p = buf.position(); p < buf.position() + buf.remaining(); p++)
        {
            if (count % 16 == 0)
            {
                if (count > 0)
                {
                    builder.append(String.format("%n"));
                }
                builder.append(String.format("%07x  ", count));
            }
            builder.append(String.format("  %02x", buf.get(p)));

            count++;
        }
        builder.append(String.format("%n"));
        builder.append(String.format("%07x%n", count));
        return builder.toString();
    }

    /**
     * Creates substitution resolver
     *
     * @param prefix Substitution prefix
     * @param substitutions Map of substituitions
     *
     * @return StringSubstitutionResolver
     */
    public static Resolver createSubstitutionResolver(final String prefix, final LinkedHashMap<String, String> substitutions)
    {
        return new StringSubstitutionResolver(prefix, substitutions);
    }

    /**
     * Resolver variable using supplied resolver
     */
    public interface Resolver
    {
        String resolve(final String variable, final Resolver resolver);
    }

    /**
     * Resolves variable from a map.
     */
    public static class MapResolver implements Resolver
    {
        private final Map<String,String> map;

        public MapResolver(final Map<String,String> map)
        {
            this.map = map;
        }

        @Override
        public String resolve(final String variable, final Resolver resolver)
        {
            return map.get(variable);
        }
    }

    /**
     * Chains two resolvers trying to resolve variable against first one and if unsuccessful against second one
     */
    public static class ChainedResolver implements Resolver
    {
        private final Resolver primary;
        private final Resolver secondary;

        public ChainedResolver(final Resolver primary, final Resolver secondary)
        {
            this.primary = primary;
            this.secondary = secondary;
        }

        @Override
        public String resolve(final String variable, final Resolver resolver)
        {
            final String result = primary.resolve(variable, resolver);
            return result != null
                    ? result
                    : secondary.resolve(variable, resolver);
        }
    }

    /**
     * Resolves substituted variables
     */
    private static class StringSubstitutionResolver implements Resolver
    {
        private final ThreadLocal<Set<String>> _stack = new ThreadLocal<>();
        private final LinkedHashMap<String, String> _substitutions;
        private final String _prefix;

        private StringSubstitutionResolver(final String prefix, final LinkedHashMap<String, String> substitutions)
        {
            _prefix = prefix;
            _substitutions = substitutions;
        }

        @Override
        public String resolve(final String variable, final Resolver resolver)
        {
            boolean clearStack = false;
            Set<String> currentStack = _stack.get();
            if (currentStack == null)
            {
                currentStack = new HashSet<>();
                _stack.set(currentStack);
                clearStack = true;
            }
            try
            {
                if (currentStack.contains(variable))
                {
                    throw new IllegalArgumentException("The value of attribute " + variable + " is defined recursively");
                }
                if (variable.startsWith(_prefix))
                {
                    currentStack.add(variable);
                    final Stack<String> stack = new Stack<>();
                    stack.add(variable);
                    String expanded = Strings.expand("${" + variable.substring(_prefix.length()) + "}", resolver,
                                                     stack, false);
                    currentStack.remove(variable);
                    if (expanded != null)
                    {
                        for (final Map.Entry<String,String> entry : _substitutions.entrySet())
                        {
                            expanded = expanded.replace(entry.getKey(), entry.getValue());
                        }
                    }
                    return expanded;
                }
                else
                {
                    return null;
                }

            }
            finally
            {
                if (clearStack)
                {
                    _stack.remove();
                }
            }
        }
    }
}
