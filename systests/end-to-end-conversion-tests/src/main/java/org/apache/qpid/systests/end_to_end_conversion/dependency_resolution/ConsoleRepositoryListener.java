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

import java.io.PrintStream;

import org.eclipse.aether.AbstractRepositoryListener;
import org.eclipse.aether.RepositoryEvent;

public class ConsoleRepositoryListener extends AbstractRepositoryListener
{

    private PrintStream _out;

    public ConsoleRepositoryListener()
    {
        this(null);
    }

    public ConsoleRepositoryListener(PrintStream out)
    {
        this._out = (out != null) ? out : System.out;
    }

    public void artifactDeployed(RepositoryEvent event)
    {
        _out.println("Deployed " + event.getArtifact() + " to " + event.getRepository());
    }

    public void artifactDeploying(RepositoryEvent event)
    {
        _out.println("Deploying " + event.getArtifact() + " to " + event.getRepository());
    }

    public void artifactDescriptorInvalid(RepositoryEvent event)
    {
        _out.println("Invalid artifact descriptor for " + event.getArtifact() + ": "
                     + event.getException().getMessage());
    }

    public void artifactDescriptorMissing(RepositoryEvent event)
    {
        _out.println("Missing artifact descriptor for " + event.getArtifact());
    }

    public void artifactInstalled(RepositoryEvent event)
    {
        _out.println("Installed " + event.getArtifact() + " to " + event.getFile());
    }

    public void artifactInstalling(RepositoryEvent event)
    {
        _out.println("Installing " + event.getArtifact() + " to " + event.getFile());
    }

    public void artifactResolved(RepositoryEvent event)
    {
        _out.println("Resolved artifact " + event.getArtifact() + " from " + event.getRepository());
    }

    public void artifactDownloading(RepositoryEvent event)
    {
        _out.println("Downloading artifact " + event.getArtifact() + " from " + event.getRepository());
    }

    public void artifactDownloaded(RepositoryEvent event)
    {
        _out.println("Downloaded artifact " + event.getArtifact() + " from " + event.getRepository());
    }

    public void artifactResolving(RepositoryEvent event)
    {
        _out.println("Resolving artifact " + event.getArtifact());
    }

    public void metadataDeployed(RepositoryEvent event)
    {
        _out.println("Deployed " + event.getMetadata() + " to " + event.getRepository());
    }

    public void metadataDeploying(RepositoryEvent event)
    {
        _out.println("Deploying " + event.getMetadata() + " to " + event.getRepository());
    }

    public void metadataInstalled(RepositoryEvent event)
    {
        _out.println("Installed " + event.getMetadata() + " to " + event.getFile());
    }

    public void metadataInstalling(RepositoryEvent event)
    {
        _out.println("Installing " + event.getMetadata() + " to " + event.getFile());
    }

    public void metadataInvalid(RepositoryEvent event)
    {
        _out.println("Invalid metadata " + event.getMetadata());
    }

    public void metadataResolved(RepositoryEvent event)
    {
        _out.println("Resolved metadata " + event.getMetadata() + " from " + event.getRepository());
    }

    public void metadataResolving(RepositoryEvent event)
    {
        _out.println("Resolving metadata " + event.getMetadata() + " from " + event.getRepository());
    }
}
