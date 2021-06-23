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
 */
package org.apache.qpid.server.user.connection.limits.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Locale;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;

public class FileParser
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileParser.class);

    private static final Character COMMENT = '#';
    private static final Character CONTINUATION = '\\';

    private static final Pattern NUMBER = Pattern.compile("\\s*(\\d+)\\s*");

    private static final String ACCESS_CONTROL = "acl";
    public static final String CONNECTION_LIMIT = "clt";
    private static final String CONFIG = "config";

    public static final String DEFAULT_FREQUENCY_PERIOD = "default_frequency_period";
    public static final String DEFAULT_FREQUENCY_PERIOD_ALTERNATIVE = "defaultfrequencyperiod";
    private static final String LOG_ALL = "log_all";
    private static final String LOG_ALL_ALTERNATIVE = "logall";

    public static final String BLOCK = "BLOCK";

    static final String UNRECOGNISED_INITIAL_TOKEN = "Unrecognised initial token '%s' at line %d";
    static final String NOT_ENOUGH_TOKENS = "Not enough tokens at line %d";
    static final String NUMBER_NOT_ALLOWED = "Number not allowed before '%s' at line %d";
    static final String CANNOT_LOAD_CONFIGURATION = "I/O Error while reading configuration";
    static final String PREMATURE_CONTINUATION = "Premature continuation character at line %d";
    static final String PARSE_TOKEN_FAILED = "Failed to parse token at line %d";
    static final String UNKNOWN_CLT_PROPERTY_MSG = "Unknown connection limit property: %s at line %d";
    static final String PROPERTY_KEY_ONLY_MSG = "Incomplete property (key only) at line %d";
    static final String PROPERTY_NO_EQUALS_MSG = "Incomplete property (no equals) at line %d";
    static final String PROPERTY_NO_VALUE_MSG = "Incomplete property (no value) at line %d";

    public static RuleSetCreator parse(String name)
    {
        return new FileParser().readAndParse(name);
    }

    public RuleSetCreator readAndParse(String name)
    {
        return readAndParse(getReaderFromURLString(name));
    }

    RuleSetCreator readAndParse(Reader reader)
    {
        final RuleSetCreator ruleSetCreator = new RuleSetCreator();

        int line = 0;
        try (final Reader fileReader = new BufferedReader(reader))
        {
            LOGGER.debug("About to load connection limit file");
            final StreamTokenizer tokenizer = new StreamTokenizer(fileReader);
            tokenizer.resetSyntax(); // setup the tokenizer

            tokenizer.commentChar(COMMENT); // single line comments
            tokenizer.eolIsSignificant(true); // return EOL as a token
            tokenizer.ordinaryChar('='); // equals is a token
            tokenizer.ordinaryChar(CONTINUATION); // continuation character (when followed by EOL)
            tokenizer.quoteChar('"'); // double quote
            tokenizer.quoteChar('\''); // single quote
            tokenizer.whitespaceChars('\u0000', '\u0020');
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
            tokenizer.wordChars('/', '/');

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
                        processLine(ruleSetCreator, line, stack);
                        break;
                    case StreamTokenizer.TT_WORD:
                        addLast(stack, tokenizer.sval);
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
            throw new IllegalConfigurationException(CANNOT_LOAD_CONFIGURATION, ioe);
        }
        catch (RuntimeException re)
        {
            throw new IllegalConfigurationException(String.format(PARSE_TOKEN_FAILED, line), re);
        }
        return ruleSetCreator;
    }

    private void processLine(RuleSetCreator ruleSetCreator, int line, Queue<String> stack)
    {
        if (stack.isEmpty())
        {
            return; // blank line
        }
        final String first = stack.poll();
        if (first == null || stack.isEmpty())
        {
            throw new IllegalConfigurationException(String.format(NOT_ENOUGH_TOKENS, line));
        }

        final Matcher matcher = NUMBER.matcher(first);
        if (matcher.matches())
        {
            final String commandType = stack.poll();
            if (ACCESS_CONTROL.equalsIgnoreCase(commandType))
            {
                parseAccessControl(stack, ruleSetCreator, line);
                stack.clear();
                return;
            }
            throw new IllegalConfigurationException(String.format(NUMBER_NOT_ALLOWED, commandType, line));
        }

        if (CONNECTION_LIMIT.equalsIgnoreCase(first))
        {
            parseConnectionLimit(stack, ruleSetCreator, line);
        }
        else if (CONFIG.equalsIgnoreCase(first))
        {
            parseConfig(stack, ruleSetCreator, line);
        }
        else if (ACCESS_CONTROL.equalsIgnoreCase(first))
        {
            parseAccessControl(stack, ruleSetCreator, line);
        }
        else
        {
            throw new IllegalConfigurationException(String.format(UNRECOGNISED_INITIAL_TOKEN, first, line));
        }
        stack.clear();
    }

    private void parseToken(StreamTokenizer tokenizer, Queue<String> stack) throws IOException
    {
        if (tokenizer.ttype == CONTINUATION)
        {
            if (tokenizer.nextToken() != StreamTokenizer.TT_EOL)
            {
                // invalid location for continuation character (add one to line because we ate the EOL)
                throw new IllegalConfigurationException(String.format(PREMATURE_CONTINUATION, tokenizer.lineno()));
            }
        }
        else if (tokenizer.ttype == '\'' || tokenizer.ttype == '"')
        {
            addLast(stack, tokenizer.sval);
        }
        else if (!Character.isWhitespace(tokenizer.ttype))
        {
            addLast(stack, Character.toString((char) tokenizer.ttype));
        }
    }

    private void addLast(Queue<String> queue, String value)
    {
        if (value != null)
        {
            queue.add(value);
        }
    }

    private void parseConnectionLimit(Queue<String> args, final RuleSetCreator ruleSetCreator, final int line)
    {
        final String identity = args.poll();
        final RulePredicates predicates = new RulePredicates();

        final Iterator<String> i = args.iterator();
        while (i.hasNext())
        {
            final String key = i.next();
            if (BLOCK.equalsIgnoreCase(key))
            {
                predicates.setBlockedUser();
            }
            else
            {
                if (predicates.parse(key, readValue(i, line)) == null)
                {
                    throw new IllegalConfigurationException(String.format(UNKNOWN_CLT_PROPERTY_MSG, key, line));
                }
            }
        }
        if (!predicates.isEmpty())
        {
            ruleSetCreator.add(Rule.newInstance(identity, predicates));
        }
    }

    private void parseAccessControl(Queue<String> args, final RuleSetCreator ruleSetCreator, final int line)
    {
        if (args.size() < 4)
        {
            return;
        }
        // poll outcome
        args.poll();
        final String identity = args.poll();
        // poll operation
        args.poll();
        // poll object
        args.poll();

        final RulePredicates predicates = new RulePredicates();
        final Iterator<String> i = args.iterator();
        while (i.hasNext())
        {
            predicates.parse(i.next(), readValue(i, line));
        }
        if (!predicates.isEmpty())
        {
            ruleSetCreator.add(Rule.newInstance(identity, predicates));
        }
    }

    private static void parseConfig(final Queue<String> args, final RuleSetCreator ruleSetCreator, final int line)
    {
        final Iterator<String> i = args.iterator();
        while (i.hasNext())
        {
            final String key = i.next().replace("-", "_").toLowerCase(Locale.ENGLISH);
            final String value = readValue(i, line);

            switch (key)
            {
                case LOG_ALL:
                case LOG_ALL_ALTERNATIVE:
                    ruleSetCreator.setLogAllMessages(Boolean.parseBoolean(value));
                    break;
                case DEFAULT_FREQUENCY_PERIOD:
                case DEFAULT_FREQUENCY_PERIOD_ALTERNATIVE:
                    ruleSetCreator.setDefaultFrequencyPeriod(Long.parseLong(value));
                    break;
                default:
            }
        }
    }

    private static String readValue(Iterator<String> i, int line)
    {
        if (!i.hasNext())
        {
            throw new IllegalConfigurationException(String.format(PROPERTY_KEY_ONLY_MSG, line));
        }
        if (!"=".equals(i.next()))
        {
            throw new IllegalConfigurationException(String.format(PROPERTY_NO_EQUALS_MSG, line));
        }
        if (!i.hasNext())
        {
            throw new IllegalConfigurationException(String.format(PROPERTY_NO_VALUE_MSG, line));
        }
        return i.next();
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
            return new InputStreamReader(Paths.get(path).toUri().toURL().openStream(), StandardCharsets.UTF_8);
        }
        catch (IOException | RuntimeException e)
        {
            throw createReaderError(path, e);
        }
    }

    private static IllegalConfigurationException createReaderError(String urlString, Exception e)
    {
        return new IllegalConfigurationException("Cannot convert " + urlString + " to a readable resource", e);
    }
}
