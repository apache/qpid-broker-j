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

define(["dojo/_base/declare",
        "dojo/_base/lang",
        "dojox/encoding/base64",
        "dojo/json",
        "dojo/request/script",
        "dojox/uuid/generateRandomUuid",
        "dojo/Deferred",
        "qpid/sasl/SaslClient"], function (declare, lang, base64, json, script, uuid, Deferred, SaslClient)
{
    return declare("qpid.sasl.CredentialBasedSaslClient", [SaslClient], {
        getResponse: function (challenge)
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
        isComplete: function ()
        {
            // summary:
            //        Returns true when no more response generation is required.
            // description:
            //        Returns true when challenge handling is complete
            // returns: boolean
            throw new TypeError("abstract");
        },
        getCredentials: function ()
        {
            // summary:
            //        Returns initial credentials
            //       to start authentication
            // description:
            //        Provides initial credentials as Promise or
            //        JSON object to start authentication process
            // returns: promise
            throw new TypeError("abstract");
        },
        toString: function ()
        {
            return "[object CredentialBasedSaslClient]";
        },
        authenticate: function (management)
        {
            var deferred = new Deferred();
            var successCallback = function (data)
            {
                deferred.resolve(data);
            };
            var failureCallback = function (data)
            {
                deferred.reject(data);
            };

            var saslClient = this;
            var processChallenge = function processChallenge(challenge)
            {
                if (saslClient.isComplete())
                {
                    successCallback(true);
                    return;
                }

                var response = null;
                try
                {
                    response = saslClient.getResponse(challenge);
                }
                catch (e)
                {
                    failureCallback(e);
                    return;
                }

                if (saslClient.isComplete() && (response == null || response == undefined))
                {
                    successCallback(true);
                }
                else
                {
                    management.sendSaslResponse(response)
                        .then(function (challenge)
                        {
                            processChallenge(challenge);
                        });
                }
            };

            dojo.when(this.getCredentials())
                .then(function (data)
                {
                    processChallenge(data);
                }, failureCallback);
            return deferred.promise;
        }
    });
});
