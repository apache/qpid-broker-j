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
package org.apache.qpid.server.util.urlstreamhandler.classpath;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

public class Handler extends URLStreamHandler
{
    public static final String PROTOCOL_HANDLER_PROPERTY = "java.protocol.handler.pkgs";
    private static boolean _registered;

    @Override
    protected URLConnection openConnection(final URL u) throws IOException
    {
        String externalForm = u.toExternalForm();
        if(externalForm.startsWith("classpath:"))
        {
            String path = externalForm.substring(10);
            URL resourceUrl = getClass().getClassLoader().getResource(path);
            if(resourceUrl == null)
            {
                throw new FileNotFoundException("No such resource found in the classpath: " + path);
            }
            return resourceUrl.openConnection();
        }
        else
        {
            throw new MalformedURLException("'"+externalForm+"' does not start with 'classpath:'");
        }
    }

    public static void register()
    {
        synchronized (System.getProperties())
        {
            if (!_registered)
            {
                String registeredPackages = System.getProperty(PROTOCOL_HANDLER_PROPERTY);
                String thisPackage = Handler.class.getPackage().getName();
                String packageToRegister = thisPackage.substring(0, thisPackage.lastIndexOf('.'));
                System.setProperty(PROTOCOL_HANDLER_PROPERTY,
                                   registeredPackages == null
                                           ? packageToRegister
                                           : packageToRegister + "|" + registeredPackages);

                _registered = true;
            }
        }
    }

}
