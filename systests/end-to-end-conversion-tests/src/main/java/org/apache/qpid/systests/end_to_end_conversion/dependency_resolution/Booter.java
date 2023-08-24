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

package org.apache.qpid.systests.end_to_end_conversion.dependency_resolution;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.repository.Proxy;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;

public class Booter
{
    private static final String FALLBACK_LOCAL_REPO_URL =
            String.join(File.pathSeparator, System.getProperty("user.home"), ".m2", "repository");
    private static final String REMOTE_REPO_URL = System.getProperty(
            "qpid.systests.end_to_end_conversion.remoteRepository",
            "https://repo.maven.apache.org/maven2/");
    private static final String LOCAL_REPO =
            System.getProperty("qpid.systests.end_to_end_conversion.localRepository", FALLBACK_LOCAL_REPO_URL);
    private static final String HTTPS_PROXY = "https_proxy";

    public static RepositorySystem newRepositorySystem()
    {
        return ManualRepositorySystemFactory.newRepositorySystem();
    }

    public static DefaultRepositorySystemSession newRepositorySystemSession(RepositorySystem system)
    {
        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();

        LocalRepository localRepo = new LocalRepository("target/local-repo");
        session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));

        session.setTransferListener(new ConsoleTransferListener());
        session.setRepositoryListener(new ConsoleRepositoryListener());

        // uncomment to generate dirty trees
        // session.setDependencyGraphTransformer( null );

        return session;
    }

    public static List<RemoteRepository> newRepositories()
    {
        return Arrays.asList(newLocalRepository(), newCentralRepository());
    }

    private static RemoteRepository newCentralRepository()
    {
        final RemoteRepository.Builder builder = new RemoteRepository.Builder("central", "default", REMOTE_REPO_URL);
        // resolve HTTPS proxy either from environment variable or from system property
        final String environmentVariable = System.getenv(HTTPS_PROXY);
        final String systemProperty = System.getProperty(HTTPS_PROXY);
        Proxy proxy = null;
        if (systemProperty != null)
        {
            proxy = getProxy(systemProperty);
        }
        else if (environmentVariable != null)
        {
            proxy = getProxy(environmentVariable);
        }
        if (proxy != null)
        {
            builder.setProxy(proxy);
        }
        return builder.build();
    }

    private static Proxy getProxy(final String proxy)
    {
        if (proxy == null)
        {
            return null;
        }
        final String httpsProxy = proxy.replace("http://", "").replace("https://", "");
        final String[] tokens = httpsProxy.split(":");
        final String host = tokens[0];
        final int port = Integer.parseInt(tokens[1]);
        return new Proxy(Proxy.TYPE_HTTPS, host, port);
    }

    private static RemoteRepository newLocalRepository()
    {
        final URL localRepoUrl = toUrl(LOCAL_REPO);
        return new RemoteRepository.Builder("local", "default", localRepoUrl.toString()).build();
    }

    private static URL toUrl(final String localRepo)
    {
        try
        {
            return new URL(localRepo);
        }
        catch (MalformedURLException e)
        {
            try
            {
                return new File(localRepo).toURI().toURL();
            }
            catch (MalformedURLException e1)
            {
                throw new RuntimeException(String.format("Failed to convert '%s' into a URL", localRepo), e);
            }
        }
    }
}
