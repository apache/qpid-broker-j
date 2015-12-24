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

define(["dojo/_base/lang"],
       function(lang)
       {
            return lang.extend( function SaslClient()
                                {
                                    // summary:
                                    //        The public interface to a SaslClient.
                                    // description:
                                    //        The public interface to a SaslClient. All SaslClient in Qpid are
                                    //        instances of this class.
                                },
                                {
                                getMechanismName: function()
                                                  {
                                                      // summary:
                                                      //        Returns mechanism name.
                                                      // description:
                                                      //        Returns mechanism name for the implementation.
                                                      // returns: string
                                                      throw new TypeError("abstract");
                                                  },
                                getResponse:      function(challenge)
                                                  {
                                                      // summary:
                                                      //        Generates response for given challenge
                                                      // description:
                                                      //        Handles given challenge represented as
                                                      //       JSON object and generates response in
                                                      //       JSON format.
                                                      //       Method can be called multiple times
                                                      //       for different challenges.
                                                      //       Throws exception on various errors or
                                                      //       authentication failures.
                                                      // returns: JSON objectSa
                                                      throw new TypeError("abstract");
                                                  },
                                isComplete:       function()
                                                  {
                                                      // summary:
                                                      //        Returns true when response for last challenge is generated.
                                                      // description:
                                                      //        Returns true when challenge handling is complete
                                                      // returns: boolean
                                                      throw new TypeError("abstract");
                                                  },
                                getPriority:      function()
                                                  {
                                                      // summary:
                                                      //        Returns SaslClient priority as integer
                                                      // description:
                                                      //        Returns SaslClient priority as integer.
                                                      //        SaslClients with highest priority is
                                                      //        chosen from multiple supported.
                                                      // returns: integer
                                                      throw new TypeError("abstract");
                                                  },
                                toString:         function()
                                                  {
                                                      // returns: string
                                                      //        Returns `[object SaslClient]`.
                                                      return "[object SaslClient]";
                                                  },
                                initialized:      function()
                                                  {
                                                      // summary:
                                                      //        Finish instance initialization.
                                                      // description:
                                                      //        Method must be called once before
                                                      //        getResponse in order to finish initialization.
                                                      //        dojo/promise/Promise is returned
                                                      // returns: promise
                                                      throw new TypeError("abstract");
                                                  },
                                getCredentials:   function()
                                                  {
                                                      // summary:
                                                      //        Returns initial credentials
                                                      //       to start authentication
                                                      // description:
                                                      //        Provides initial credentials as Promise or
                                                      //        JSON object to start authentication process
                                                      // returns: promise
                                                      throw new TypeError("abstract");
                                                  }
                                });
       });
