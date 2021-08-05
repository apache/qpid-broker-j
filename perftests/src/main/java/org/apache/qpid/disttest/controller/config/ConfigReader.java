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
package org.apache.qpid.disttest.controller.config;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.disttest.json.ObjectMapperFactory;


public class ConfigReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigReader.class);

    public Config getConfigFromFile(String fileName) throws IOException
    {
        Reader reader = getConfigReader(fileName);

        Config config = readConfig(reader);
        return config;
    }

    public Config readConfig(Reader reader) throws IOException
    {
        return readConfig(reader, false);
    }

    public Config readConfig(Reader reader, boolean isJavascript) throws IOException
    {
        if (isJavascript)
        {
            return readJson(new StringReader(new JavaScriptConfigEvaluator().evaluateJavaScript(reader)));
        }
        else
        {
            return readJson(reader);
        }
    }

    private Reader getConfigReader(String fileName) throws IOException
    {
        Reader reader = null;
        if (fileName.endsWith(".js"))
        {
            LOGGER.debug("Evaluating javascript: {}", fileName);
            reader = new StringReader(new JavaScriptConfigEvaluator().evaluateJavaScript(fileName));
        }
        else
        {
            LOGGER.debug("Loading JSON: {}", fileName);
            reader = new FileReader(fileName);
        }
        return reader;
    }


    private Config readJson(Reader reader) throws IOException
    {
        final ObjectMapper objectMapper = new ObjectMapperFactory().createObjectMapper();

        return objectMapper.readValue(reader, Config.class);
    }

}
