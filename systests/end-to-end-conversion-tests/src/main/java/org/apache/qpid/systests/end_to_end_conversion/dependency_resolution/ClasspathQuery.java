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
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyFilter;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;

public class ClasspathQuery
{
    private static final LoadingCache<Collection<String>, List<File>> _classpathCache;
    private static final RepositorySystem _mavenRepositorySystem;
    private static final RepositorySystemSession _mavenRepositorySession;

    static
    {
        _mavenRepositorySystem = Booter.newRepositorySystem();
        _mavenRepositorySession = Booter.newRepositorySystemSession(_mavenRepositorySystem);
        _classpathCache = CacheBuilder.newBuilder()
                                      .maximumSize(8)
                                      .recordStats()
                                      .build(new CacheLoader<Collection<String>, List<File>>()
                                      {
                                          @Override
                                          public List<File> load(final Collection<String> key) throws Exception
                                          {
                                              return doBuildClassPath(key);
                                          }
                                      });
    }

    private final Class<?> _clientClass;
    private final Collection<String> _clientGavs;


    public ClasspathQuery(final Class<?> clientClass, final Collection<String> gavs)
    {
        _clientClass = clientClass;
        _clientGavs = gavs;
    }

    public static String getCacheStats()
    {
        return _classpathCache.stats().toString();
    }

    private static List<File> doBuildClassPath(final Collection<String> gavs)
    {
        return Collections.unmodifiableList(new ArrayList<>(getJarFiles(gavs)));
    }

    private static Set<File> getJarFiles(final Collection<String> gavs)
    {
        Set<File> jars = new HashSet<>();

        for (final String gav : gavs)
        {
            Artifact artifact = new DefaultArtifact(gav);

            DependencyFilter classpathFlter = DependencyFilterUtils.classpathFilter(JavaScopes.COMPILE);

            CollectRequest collectRequest = new CollectRequest();
            collectRequest.setRoot(new Dependency(artifact, JavaScopes.COMPILE));
            collectRequest.setRepositories(Booter.newRepositories());

            DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, classpathFlter);

            List<ArtifactResult> artifactResults = null;
            try
            {
                artifactResults = _mavenRepositorySystem.resolveDependencies(_mavenRepositorySession, dependencyRequest)
                                                        .getArtifactResults();
            }
            catch (DependencyResolutionException e)
            {
                throw new RuntimeException(String.format("Error while dependency resolution for '%s'", gav), e);
            }

            if (artifactResults == null)
            {
                throw new RuntimeException(String.format("Could not resolve dependency for '%s'", gav));
            }

            for (ArtifactResult artifactResult : artifactResults)
            {
                System.out.println(artifactResult.getArtifact() + " resolved to "
                                   + artifactResult.getArtifact().getFile());
            }

            jars.addAll(artifactResults.stream()
                                       .map(result -> result.getArtifact().getFile())
                                       .collect(Collectors.toSet()));
        }
        return jars;
    }

    public Class<?> getClientClass()
    {
        return _clientClass;
    }

    public Collection<String> getClientGavs()
    {
        return _clientGavs;
    }

    public String getClasspath()
    {
        return buildClassPath(_clientClass, _clientGavs);
    }

    private String buildClassPath(final Class<?> clientClazz, final Collection<String> gavs)
    {
        final List<File> classpathElements = new ArrayList<>();
        final List<File> cached = _classpathCache.getUnchecked(gavs);
        if (cached != null)
        {
            classpathElements.addAll(cached);
        }
        classpathElements.add(getLocalClasspathElement(clientClazz));

        final String collect = classpathElements.stream()
                                                .map(File::toString)
                                                .collect(Collectors.joining(System.getProperty("path.separator")));
        return collect;
    }

    private File getLocalClasspathElement(final Class<?> clazz)
    {
        int packageDepth = getPackageDepth(clazz);
        final URL resource = clazz.getResource("/" + clazz.getName().replace(".", "/") + ".class");
        // TODO handle JAR case
        try
        {
            Path path = new File(resource.toURI()).toPath();
            for (int i = 0; i < packageDepth + 1; ++i)
            {
                path = path.getParent();
            }

            return path.toFile();
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException(String.format("Failed to get classpath element for %s", clazz), e);
        }
    }

    private int getPackageDepth(Class clazz)
    {
        final String publisherClassName = clazz.getName();
        int lastIndex = 0;
        int count = 0;

        while (lastIndex != -1)
        {
            lastIndex = publisherClassName.indexOf(".", lastIndex);

            if (lastIndex != -1)
            {
                count++;
                lastIndex += 1;
            }
        }
        return count;
    }
}
