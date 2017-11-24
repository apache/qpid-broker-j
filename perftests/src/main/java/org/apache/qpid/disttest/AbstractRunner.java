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
package org.apache.qpid.disttest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class AbstractRunner
{
    public static final String JNDI_CONFIG_PROP = "jndi-config";
    public static final String JNDI_CONFIG_DEFAULT = "perftests-jndi.properties";

    private Map<String,String> _cliOptions = new HashMap<String, String>();
    {
        getCliOptions().put(JNDI_CONFIG_PROP, JNDI_CONFIG_DEFAULT);
    }

    protected Context getContext()
    {
        String jndiConfig = getJndiConfig();

        try
        {
            Properties properties = new Properties();
            properties.put(Context.PROVIDER_URL, jndiConfig);
            try(InputStream is = getJndiConfigurationInputStream(jndiConfig))
            {
                properties.load(is);
            }

            return  new InitialContext(properties);
        }
        catch (IOException | NamingException e)
        {
            throw new DistributedTestException("Exception whilst creating InitialContext from URL '"
                                               + jndiConfig + "'", e);
        }
    }

    public void parseArgumentsIntoConfig(String[] args)
    {
        ArgumentParser argumentParser = new ArgumentParser();
        argumentParser.parseArgumentsIntoConfig(getCliOptions(), args);
    }

    protected String getJndiConfig()
    {
        return getCliOptions().get(AbstractRunner.JNDI_CONFIG_PROP);
    }

    protected Map<String,String> getCliOptions()
    {
        return _cliOptions;
    }

    private InputStream getJndiConfigurationInputStream(final String providerUrl) throws IOException
    {
        try
        {
            URL url = new URL(providerUrl);
            return url.openStream();
        }
        catch (MalformedURLException mue)
        {
            return new FileInputStream(new File(providerUrl));
        }
    }
}
