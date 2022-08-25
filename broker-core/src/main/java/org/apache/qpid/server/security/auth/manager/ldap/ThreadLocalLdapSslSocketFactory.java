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
package org.apache.qpid.server.security.auth.manager.ldap;

import javax.net.ssl.SSLSocketFactory;

public class ThreadLocalLdapSslSocketFactory
{
    private static final ThreadLocal<SSLSocketFactory> THREAD_LOCAL = new ThreadLocal<>();

    /** Shouldn't be instantiated directly */
    private ThreadLocalLdapSslSocketFactory()
    {

    }

    public static SSLSocketFactory getDefault()
    {
        final SSLSocketFactory result = THREAD_LOCAL.get();
        if (result == null)
        {
            throw new IllegalStateException("SSLSocketFactory instance not available");
        }
        return result;
    }

    public static void set(final SSLSocketFactory factory)
    {
        THREAD_LOCAL.set(factory);
    }

    public static void remove()
    {
        THREAD_LOCAL.remove();
    }
}
