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

define(["dojo/_base/lang"], function (lang)
{
    return lang.extend(function SaslClient()
    {
        // summary:
        //        The public interface to a SaslClient.
        // description:
        //        The public interface to a SaslClient. All SaslClient in Qpid are
        //        instances of this class.
    }, {
        getMechanismName: function ()
        {
            // summary:
            //        Returns mechanism name.
            // description:
            //        Returns mechanism name for the implementation.
            // returns: string
            throw new TypeError("abstract");
        },
        init: function(data)
        {
            // summary:
            //        Initialises client with intial data including credentials.
            // description:
            //        Initialises client with intial data including credentials.
            // returns: promise
            throw new TypeError("abstract");
        },
        getInitialResponse: function()
        {
            // summary:
            //        Returns initial response or null if no initial response
            // description:
            //        Returns initial response or null if no initial response
            // returns: string
            throw new TypeError("abstract");
        },
        getResponse: function (challenge)
        {
            // summary:
            //        Generates response for given challenge
            // description:
            //        Generates response for given challenge
            // returns: string
            throw new TypeError("abstract");
        },
        isComplete: function ()
        {
            // summary:
            //        Returns true if SASL negotiation is complete
            // description:
            //        Returns true if SASL negotiation is complete
            // returns: boolean
            throw new TypeError("abstract");
        },
        getPriority: function ()
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
        toString: function ()
        {
            // returns: string
            //        Returns `[object SaslClient]`.
            return "[object SaslClient]";
        }
    });
});
