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
package org.apache.qpid.server.security;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

class CompoundSecurityToken implements SecurityToken
{
    private final Subject _subject;
    private final AtomicReference<CompoundTokenMapReference> _reference = new AtomicReference<>();

    CompoundSecurityToken(final List<AccessControl<?>> accessControls, final Subject subject)
    {
        _subject = subject;
        CompoundTokenMapReference compoundTokenMapReference = new CompoundTokenMapReference(accessControls);
        _reference.set(compoundTokenMapReference);
        compoundTokenMapReference.init(subject);
    }


    Map<AccessControl<?>, SecurityToken> getCompoundToken(final List<AccessControl<?>> accessControls)
    {
        CompoundTokenMapReference ref = _reference.get();
        if (ref.getAccessControlList() != accessControls)
        {
            CompoundTokenMapReference oldRef = ref;
            ref = new CompoundTokenMapReference(accessControls);
            ref.init(_subject);
            _reference.compareAndSet(oldRef, ref);
        }
        return ref.getCompoundTokenMap();
    }

    private static class CompoundTokenMapReference
    {
        private final List<AccessControl<?>> _accessControlList;
        private final Map<AccessControl<?>, SecurityToken> _compoundTokenMap;

        private CompoundTokenMapReference(final List<AccessControl<?>> accessControlList)
        {
            _accessControlList = accessControlList;
            _compoundTokenMap = new ConcurrentHashMap<>();
        }

        public synchronized void init(Subject subject)
        {
            if(_compoundTokenMap.isEmpty())
            {
                for (AccessControl accessControl : _accessControlList)
                {
                    if(accessControl != null)
                    {
                        SecurityToken token = accessControl.newToken(subject);
                        if(token != null)
                        {
                            _compoundTokenMap.put(accessControl, token);
                        }
                    }
                }
            }
        }

        public List<AccessControl<?>> getAccessControlList()
        {
            return _accessControlList;
        }

        public Map<AccessControl<?>, SecurityToken> getCompoundTokenMap()
        {
            return _compoundTokenMap;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            final CompoundTokenMapReference that = (CompoundTokenMapReference) o;
            return Objects.equals(_accessControlList, that._accessControlList) &&
                   Objects.equals(_compoundTokenMap, that._compoundTokenMap);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(_accessControlList, _compoundTokenMap);
        }
    }

}
