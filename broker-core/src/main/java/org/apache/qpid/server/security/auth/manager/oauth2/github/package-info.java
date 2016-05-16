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

/**
 * Identity resolver utilising GitHub's OAuth 2.0 user API
 * <p>
 * To use GitHub as an authentication provider, the OAuth2Authentication
 * needs to be configured to co-operate with the identity resolver like so:
 *
 * <pre>
 * "type" : "OAuth2",
 * "authorizationEndpointURI" : "https://github.com/login/oauth/authorize",
 * "tokenEndpointURI" : "https://github.com/login/oauth/access_token",
 * "tokenEndpointNeedsAuth" : false,
 * "identityResolverType" : "GitHubUser",
 * "identityResolverEndpointURI" : "https://api.github.com/user",
 * "clientId" : "......",
 * "clientSecret" : "....",
 * "scope" : "user"
 * </pre>
 */
package org.apache.qpid.server.security.auth.manager.oauth2.github;
