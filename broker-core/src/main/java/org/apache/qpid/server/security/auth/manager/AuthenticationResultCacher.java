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

package org.apache.qpid.server.security.auth.manager;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SocketConnectionPrincipal;
import org.apache.qpid.server.util.StringUtil;

public class AuthenticationResultCacher
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticationResultCacher.class);
    private static final Charset UTF8 = StandardCharsets.UTF_8;

    private final Cache<String, AuthenticationResult> _authenticationCache;
    private final int _iterationCount;

    public AuthenticationResultCacher(int cacheSize, long expirationTime, int iterationCount)
    {
        if (cacheSize <= 0 || expirationTime <= 0 || iterationCount < 0)
        {
            LOGGER.debug("disabling authentication result caching");
            _iterationCount = 0;
            _authenticationCache = null;
        }
        else
        {
            _iterationCount = iterationCount;
            _authenticationCache = CacheBuilder.newBuilder()
                                               .maximumSize(cacheSize)
                                               .expireAfterWrite(expirationTime, TimeUnit.SECONDS)
                                               .build();
        }
    }

    public AuthenticationResult getOrLoad(final String[] credentials, final Callable<AuthenticationResult> loader)
    {
        try
        {
            if (_authenticationCache == null)
            {
                return loader.call();
            }
            else
            {
                String credentialDigest = digestCredentials(credentials);
                return _authenticationCache.get(credentialDigest, new Callable<AuthenticationResult>()
                {
                    @Override
                    public AuthenticationResult call() throws Exception
                    {
                        return loader.call();
                    }
                });
            }
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException("Unexpected checked Exception while authenticating", e.getCause());
        }
        catch (UncheckedExecutionException e)
        {
            throw new RuntimeException("Unexpected Exception while authenticating", e.getCause());
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unexpected checked Exception while authenticating", e);
        }
    }

    private String digestCredentials(final String... content)
    {
        try
        {
            MessageDigest md = MessageDigest.getInstance("SHA-256");

            Subject subject = Subject.getSubject(AccessController.getContext());
            Set<SocketConnectionPrincipal> connectionPrincipals = subject.getPrincipals(SocketConnectionPrincipal.class);
            if (connectionPrincipals != null && !connectionPrincipals.isEmpty())
            {
                SocketConnectionPrincipal connectionPrincipal = connectionPrincipals.iterator().next();
                SocketAddress remoteAddress = connectionPrincipal.getRemoteAddress();
                String address;
                if (remoteAddress instanceof InetSocketAddress)
                {
                    address = ((InetSocketAddress) remoteAddress).getHostString();
                }
                else
                {
                    address = remoteAddress.toString();
                }
                if (address != null)
                {
                    md.update(address.getBytes(UTF8));
                }
            }

            for (String part : content)
            {
                md.update(part.getBytes(UTF8));
            }

            byte[] credentialDigest = md.digest();
            for (int i = 0; i < _iterationCount; ++i)
            {
                md = MessageDigest.getInstance("SHA-256");
                credentialDigest = md.digest(credentialDigest);
            }

            return StringUtil.toHex(credentialDigest);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException("JVM is non compliant. Seems to not support SHA-256.");
        }
    }
}
