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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;

public class Booter
{
    private static final String FALLBACK_LOCAL_REPO_URL = Stream.of(System.getProperty("user.home"),
                                                                    ".m2", "repository")
                                                                .collect(Collectors.joining(File.pathSeparator));
    private static final String REMOTE_REPO_URL = System.getProperty(
            "qpid.systests.end_to_end_conversion.remoteRepository",
            "https://repo.maven.apache.org/maven2/");
    private static final String LOCAL_REPO =
            System.getProperty("qpid.systests.end_to_end_conversion.localRepository", FALLBACK_LOCAL_REPO_URL);

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
        return new RemoteRepository.Builder("central", "default", REMOTE_REPO_URL).build();
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
