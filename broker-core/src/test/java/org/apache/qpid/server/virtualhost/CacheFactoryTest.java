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
package org.apache.qpid.server.virtualhost;


import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.security.PrivilegedAction;

import javax.security.auth.Subject;

import com.google.common.cache.Cache;
import org.junit.Test;

import org.apache.qpid.server.model.VirtualHost;

public class CacheFactoryTest
{

    @Test
    public void getCache()
    {
        String cacheName = "test";
        final Cache<Object, Object> cache = new NullCache<>();
        final CacheProvider virtualHost = mock(CacheProvider.class, withSettings().extraInterfaces(VirtualHost.class));
        when(virtualHost.getNamedCache(cacheName)).thenReturn(cache);
        final Subject subject = new Subject();
        subject.getPrincipals().add(new VirtualHostPrincipal((VirtualHost<?>) virtualHost));
        subject.setReadOnly();

        Cache<String, String> actualCache = Subject.doAs(subject,
                                                   (PrivilegedAction<Cache<String, String>>) () -> CacheFactory.getCache(cacheName,
                                                                                                                         null));
        assertSame(actualCache, cache);
        verify(virtualHost).getNamedCache(cacheName);
    }
}
