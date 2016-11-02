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
package org.apache.qpid.server;

import org.apache.qpid.server.model.SystemConfig;

public interface SystemLauncherListener
{
    void beforeStartup();
    void errorOnStartup(RuntimeException e);
    void afterStartup();
    void onContainerResolve(SystemConfig<?> systemConfig);
    void onContainerClose(SystemConfig<?> systemConfig);
    void onShutdown(int exitCode);

    void exceptionOnShutdown(Exception e);

    class DefaultSystemLauncherListener implements SystemLauncherListener
    {
        @Override
        public void beforeStartup()
        {

        }

        @Override
        public void errorOnStartup(final RuntimeException e)
        {

        }

        @Override
        public void afterStartup()
        {

        }

        @Override
        public void onContainerResolve(final SystemConfig<?> systemConfig)
        {

        }

        @Override
        public void onContainerClose(final SystemConfig<?> systemConfig)
        {

        }

        @Override
        public void onShutdown(final int exitCode)
        {

        }

        @Override
        public void exceptionOnShutdown(final Exception e)
        {
        }
    }


    class ChainedSystemLauncherListener implements SystemLauncherListener
    {
        private final SystemLauncherListener[] _listeners;

        public ChainedSystemLauncherListener(SystemLauncherListener... chain)
        {
            _listeners = chain;
        }

        @Override
        public void beforeStartup()
        {
            for (SystemLauncherListener listener : _listeners)
            {
                listener.beforeStartup();
            }
        }

        @Override
        public void errorOnStartup(final RuntimeException e)
        {
            for (SystemLauncherListener listener : _listeners)
            {
                listener.errorOnStartup(e);
            }
        }

        @Override
        public void afterStartup()
        {
            for (SystemLauncherListener listener : _listeners)
            {
                listener.afterStartup();
            }
        }

        @Override
        public void onContainerResolve(final SystemConfig<?> systemConfig)
        {
            for (SystemLauncherListener listener : _listeners)
            {
                listener.onContainerResolve(systemConfig);
            }
        }

        @Override
        public void onContainerClose(final SystemConfig<?> systemConfig)
        {
            for (SystemLauncherListener listener : _listeners)
            {
                listener.onContainerClose(systemConfig);
            }
        }

        @Override
        public void onShutdown(final int exitCode)
        {
            for(SystemLauncherListener listener : _listeners)
            {
                listener.onShutdown(exitCode);
            }
        }

        @Override
        public void exceptionOnShutdown(final Exception e)
        {
            for(SystemLauncherListener listener : _listeners)
            {
                listener.exceptionOnShutdown(e);
            }
        }
    }
}
