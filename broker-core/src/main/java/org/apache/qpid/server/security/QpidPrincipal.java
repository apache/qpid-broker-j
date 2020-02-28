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

package org.apache.qpid.server.security;

import java.io.Serializable;
import java.security.Principal;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.qpid.server.model.ConfiguredObject;


public interface QpidPrincipal extends Principal, Serializable
{
    static <P extends Principal> P getSingletonPrincipal(final Subject authSubject,
                                                         final boolean isPrincipalOptional,
                                                         final Class<P> principalClazz)
    {
        if (authSubject == null)
        {
            throw new IllegalArgumentException("No authenticated subject.");
        }

        final Set<P> principals = authSubject.getPrincipals(principalClazz);
        int numberOfAuthenticatedPrincipals = principals.size();

        if(numberOfAuthenticatedPrincipals == 0 && isPrincipalOptional)
        {
            return null;
        }
        else
        {
            if (numberOfAuthenticatedPrincipals != 1)
            {
                throw new IllegalArgumentException(
                        String.format(
                                "Can't find single %s in the authenticated subject. There were %d "
                                + "%s principals out of a total number of principals of: %s",
                                principalClazz.getSimpleName(),
                                numberOfAuthenticatedPrincipals,
                                principalClazz.getSimpleName(),
                                authSubject.getPrincipals()));
            }
            return principals.iterator().next();
        }
    }

    ConfiguredObject<?> getOrigin();


}
