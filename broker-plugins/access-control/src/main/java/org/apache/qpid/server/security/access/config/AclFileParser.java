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
package org.apache.qpid.server.security.access.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.config.Rule.Builder;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AclFileParser
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AclFileParser.class);

    public static final String DEFAULT_ALLOW = "defaultallow";
    public static final String DEFAULT_DEFER = "defaultdefer";
    public static final String DEFAULT_DENY = "defaultdeny";

    private static final Character COMMENT = '#';
    private static final Character CONTINUATION = '\\';

    public static final String ACL = "acl";
    private static final String CONFIG = "config";
    private static final String GROUP = "GROUP";

    static final String UNRECOGNISED_INITIAL_MSG = "Unrecognised initial token '%s' at line %d";
    static final String NOT_ENOUGH_TOKENS_MSG = "Not enough tokens at line %d";
    static final String NUMBER_NOT_ALLOWED_MSG = "Number not allowed before '%s' at line %d";
    static final String CANNOT_LOAD_MSG = "I/O Error while reading configuration";
    static final String PREMATURE_CONTINUATION_MSG = "Premature continuation character at line %d";
    static final String PARSE_TOKEN_FAILED_MSG = "Failed to parse token at line %d";
    static final String NOT_ENOUGH_ACL_MSG = "Not enough data for an acl at line %d";
    static final String NOT_ENOUGH_CONFIG_MSG = "Not enough data for config at line %d";
    static final String BAD_ACL_RULE_NUMBER_MSG = "Invalid rule number at line %d";
    static final String PROPERTY_KEY_ONLY_MSG = "Incomplete property (key only) at line %d";
    static final String PROPERTY_NO_EQUALS_MSG = "Incomplete property (no equals) at line %d";
    static final String PROPERTY_NO_VALUE_MSG = "Incomplete property (no value) at line %d";
    static final String GROUP_NOT_SUPPORTED = "GROUP keyword not supported at line %d." +
            " Groups should defined via a Group Provider, not in the ACL file.";
    static final String PROPERTY_NO_CLOSE_BRACKET_MSG = "Incomplete property (no close bracket) at line %d";

    private static final String INVALID_ENUM = "Not a valid %s: %s";
    private static final String INVALID_URL = "Cannot convert %s to a readable resource";

    private static final Map<String, RuleOutcome> PERMISSION_MAP;
    private static final Map<String, LegacyOperation> OPERATION_MAP;
    private static final Map<String, ObjectType> OBJECT_TYPE_MAP;

    private static final Pattern NUMBER = Pattern.compile("\\s*(\\d+)\\s*");

    static
    {
        PERMISSION_MAP = new HashMap<>();
        for (final RuleOutcome value : RuleOutcome.values())
        {
            PERMISSION_MAP.put(value.name().toUpperCase(Locale.ENGLISH), value);
            PERMISSION_MAP.put(value.name().replace('_', '-').toUpperCase(Locale.ENGLISH), value);
        }

        OPERATION_MAP = Arrays.stream(LegacyOperation.values()).collect(
                Collectors.toMap(value -> value.name().toUpperCase(Locale.ENGLISH), Function.identity()));

        OBJECT_TYPE_MAP = Arrays.stream(ObjectType.values()).collect(
                Collectors.toMap(value -> value.name().toUpperCase(Locale.ENGLISH), Function.identity()));
    }

    public static RuleSet parse(String name, EventLoggerProvider eventLogger)
    {
        return new AclFileParser().readAndParse(name, eventLogger);
    }

    public static RuleSet parse(Reader reader, EventLoggerProvider eventLogger)
    {
        return new AclFileParser().readAndParse(reader, eventLogger);
    }

    RuleSet readAndParse(String name, EventLoggerProvider eventLogger)
    {
        return readAndParse(getReaderFromURLString(name)).createRuleSet(eventLogger);
    }

    RuleSet readAndParse(Reader reader, EventLoggerProvider eventLogger)
    {
        return readAndParse(reader).createRuleSet(eventLogger);
    }

    RuleCollector readAndParse(final Reader configReader)
    {
        final RuleCollector ruleCollector = new RuleCollector();

        int line = 0;
        try (Reader fileReader = configReader)
        {
            LOGGER.debug("About to load ACL file");
            final StreamTokenizer tokenizer = new StreamTokenizer(new BufferedReader(fileReader));
            tokenizer.resetSyntax(); // setup the tokenizer

            tokenizer.commentChar(COMMENT); // single line comments
            tokenizer.eolIsSignificant(true); // return EOL as a token
            tokenizer.ordinaryChar('='); // equals is a token
            tokenizer.ordinaryChar(CONTINUATION); // continuation character (when followed by EOL)
            tokenizer.quoteChar('"'); // double quote
            tokenizer.quoteChar('\''); // single quote
            tokenizer.whitespaceChars('\u0000', '\u0020'); // whitespace (to be ignored) TODO properly
            tokenizer.wordChars('a', 'z'); // unquoted token characters [a-z]
            tokenizer.wordChars('A', 'Z'); // [A-Z]
            tokenizer.wordChars('0', '9'); // [0-9]
            tokenizer.wordChars('_', '_'); // underscore
            tokenizer.wordChars('-', '-'); // dash
            tokenizer.wordChars('.', '.'); // dot
            tokenizer.wordChars('*', '*'); // star
            tokenizer.wordChars('@', '@'); // at
            tokenizer.wordChars(':', ':'); // colon
            tokenizer.wordChars('+', '+'); // plus

            // parse the acl file lines
            final Queue<String> stack = new ArrayDeque<>();
            int current;
            do
            {
                current = tokenizer.nextToken();
                line = tokenizer.lineno() - 1;
                switch (current)
                {
                    case StreamTokenizer.TT_EOF:
                    case StreamTokenizer.TT_EOL:
                        processLine(ruleCollector, line, stack);
                        break;
                    case StreamTokenizer.TT_NUMBER:
                        // Dead code because the parsing numbers is turned off, see StreamTokenizer::parseNumbers.
                        addLast(stack, Integer.toString(Double.valueOf(tokenizer.nval).intValue()));
                        break;
                    case StreamTokenizer.TT_WORD:
                        addLast(stack, tokenizer.sval); // token
                        break;
                    default:
                        parseToken(tokenizer, stack);
                }
            }
            while (current != StreamTokenizer.TT_EOF);
        }
        catch (IllegalConfigurationException ice)
        {
            throw ice;
        }
        catch (IOException ioe)
        {
            throw new IllegalConfigurationException(CANNOT_LOAD_MSG, ioe);
        }
        catch (RuntimeException re)
        {
            throw new IllegalConfigurationException(String.format(PARSE_TOKEN_FAILED_MSG, line), re);
        }
        return ruleCollector;
    }

    private void processLine(RuleCollector ruleCollector, int line, Queue<String> stack)
    {
        if (stack.isEmpty())
        {
            return;
        }

        // pull out the first token from the bottom of the stack and check arguments exist
        final String first = stack.poll();
        if (stack.isEmpty())
        {
            throw new IllegalConfigurationException(String.format(NOT_ENOUGH_TOKENS_MSG, line));
        }

        // check for and parse optional initial number for ACL lines
        final Matcher matcher = NUMBER.matcher(first);
        if (matcher.matches())
        {
            // get the next element and set the acl number
            final String type = stack.poll();
            if (ACL.equalsIgnoreCase(type))
            {
                final Integer number = validateNumber(Integer.valueOf(matcher.group()), ruleCollector, line);
                parseAcl(number, stack, ruleCollector, line);
                // reset stack, start next line
                stack.clear();
                return;
            }
            throw new IllegalConfigurationException(String.format(NUMBER_NOT_ALLOWED_MSG, type, line));
        }

        if (ACL.equalsIgnoreCase(first))
        {
            parseAcl(null, stack, ruleCollector, line);
        }
        else if (CONFIG.equalsIgnoreCase(first))
        {
            parseConfig(stack, ruleCollector, line);
        }
        else if (GROUP.equalsIgnoreCase(first))
        {
            throw new IllegalConfigurationException(String.format(GROUP_NOT_SUPPORTED, line));
        }
        else
        {
            throw new IllegalConfigurationException(String.format(UNRECOGNISED_INITIAL_MSG, first, line));
        }

        // reset stack, start next line
        stack.clear();
    }

    private void parseToken(StreamTokenizer tokenizer, Queue<String> stack) throws IOException
    {
        if (tokenizer.ttype == CONTINUATION)
        {
            if (tokenizer.nextToken() != StreamTokenizer.TT_EOL)
            {
                // invalid location for continuation character (add one to line because we ate the EOL)
                throw new IllegalConfigurationException(String.format(PREMATURE_CONTINUATION_MSG, tokenizer.lineno()));
            }
            // continue reading next line
        }
        else if (tokenizer.ttype == '\'' || tokenizer.ttype == '"')
        {
            addLast(stack, tokenizer.sval); // quoted token
        }
        else if (!Character.isWhitespace(tokenizer.ttype))
        {
            addLast(stack, Character.toString((char) tokenizer.ttype)); // single character
        }
    }

    private void addLast(Queue<String> queue, String value)
    {
        if (value != null)
        {
            queue.add(value);
        }
    }

    private Integer validateNumber(Integer number, RuleCollector ruleCollector, int line)
    {
        if (!ruleCollector.isValidNumber(number))
        {
            throw new IllegalConfigurationException(String.format(BAD_ACL_RULE_NUMBER_MSG, line));
        }
        return number;
    }

    private void parseAcl(Integer number, Queue<String> args, final RuleCollector ruleCollector, final int line)
    {
        if (args.size() < 3)
        {
            throw new IllegalConfigurationException(String.format(NOT_ENOUGH_ACL_MSG, line));
        }

        final Builder builder = new Builder()
                .withOutcome(parsePermission(args.poll(), line))
                .withIdentity(args.poll())
                .withOperation(parseOperation(args.poll(), line));

        if (!args.isEmpty())
        {
            builder.withObject(parseObjectType(args.poll(), line));

            final Iterator<String> tokenIterator = args.iterator();
            while (tokenIterator.hasNext())
            {
                builder.withPredicate(tokenIterator.next(), readValue(tokenIterator, line));
            }
        }
        ruleCollector.addRule(number, builder.build());
    }

    private static void parseConfig(final Queue<String> args, final RuleCollector ruleCollector, final int line)
    {
        if (args.size() < 3)
        {
            throw new IllegalConfigurationException(String.format(NOT_ENOUGH_CONFIG_MSG, line));
        }

        final Iterator<String> i = args.iterator();
        while (i.hasNext())
        {
            final String key = i.next().toLowerCase(Locale.ENGLISH);
            final Set<String> values = readValue(i, line);
            final Boolean value = Boolean.valueOf(Iterables.getOnlyElement(values));

            if (Boolean.TRUE.equals(value))
            {
                switch (key)
                {
                    case DEFAULT_ALLOW:
                        ruleCollector.setDefaultResult(Result.ALLOWED);
                        break;
                    case DEFAULT_DEFER:
                        ruleCollector.setDefaultResult(Result.DEFER);
                        break;
                    case DEFAULT_DENY:
                        ruleCollector.setDefaultResult(Result.DENIED);
                        break;
                    default:
                }
            }
        }
    }

    private static Set<String> readValue(Iterator<String> tokenIterator, int line)
    {
        if (!tokenIterator.hasNext())
        {
            throw new IllegalConfigurationException(String.format(PROPERTY_KEY_ONLY_MSG, line));
        }
        if (!"=".equals(tokenIterator.next()))
        {
            throw new IllegalConfigurationException(String.format(PROPERTY_NO_EQUALS_MSG, line));
        }
        if (!tokenIterator.hasNext())
        {
            throw new IllegalConfigurationException(String.format(PROPERTY_NO_VALUE_MSG, line));
        }
        final String value = tokenIterator.next();
        if ("[".equals(value))
        {
            final Set<String> values = new HashSet<>();
            String next;
            while (tokenIterator.hasNext())
            {
                next = tokenIterator.next();
                if (AclRulePredicates.SEPARATOR.equals(next))
                {
                    continue;
                }
                if ("]".equals(next))
                {
                    return values;
                }
                values.add(next);
            }
            throw new IllegalConfigurationException(String.format(PROPERTY_NO_CLOSE_BRACKET_MSG, line));
        }
        return Collections.singleton(value);
    }

    private RuleOutcome parsePermission(final String text, final int line)
    {
        return parseEnum(PERMISSION_MAP, text, line, "permission");
    }

    private LegacyOperation parseOperation(final String text, final int line)
    {
        return parseEnum(OPERATION_MAP, text, line, "operation");
    }

    private ObjectType parseObjectType(final String text, final int line)
    {
        return parseEnum(OBJECT_TYPE_MAP, text, line, "object type");
    }

    private <T extends Enum<T>> T parseEnum(final Map<String, T> map,
                                            final String text,
                                            final int line,
                                            final String typeDescription)
    {
        return Optional.ofNullable(map.get(text.toUpperCase(Locale.ENGLISH)))
                .orElseThrow(() -> new IllegalConfigurationException(String.format(PARSE_TOKEN_FAILED_MSG, line),
                        new IllegalArgumentException(String.format(INVALID_ENUM, typeDescription, text))));
    }

    private Reader getReaderFromURLString(String urlString)
    {
        try
        {
            return new InputStreamReader(new URL(urlString).openStream(), StandardCharsets.UTF_8);
        }
        catch (MalformedURLException e)
        {
            return getReaderFromPath(urlString);
        }
        catch (IOException | RuntimeException e)
        {
            throw createReaderError(urlString, e);
        }
    }

    private Reader getReaderFromPath(String path)
    {
        try
        {
            final Path file = Paths.get(path);
            return new InputStreamReader(file.toUri().toURL().openStream(), StandardCharsets.UTF_8);
        }
        catch (IOException | RuntimeException e)
        {
            throw createReaderError(path, e);
        }
    }

    private IllegalConfigurationException createReaderError(String urlString, Exception e)
    {
        return new IllegalConfigurationException(String.format(INVALID_URL, urlString), e);
    }
}
