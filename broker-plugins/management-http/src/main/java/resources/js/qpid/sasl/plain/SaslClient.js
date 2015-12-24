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
        "dojo/Deferred",
        "qpid/sasl/SaslClient",
        "qpid/sasl/UsernamePasswordProvider"],
       function(declare, lang, base64, Deferred, SaslClient, UsernamePasswordProvider)
       {
            var deferred = new Deferred();
            deferred.resolve("initialized");
            return declare("qpid.sasl.SaslClientPlain", [SaslClient], {
                 _state:             "initial",
                 getMechanismName:   function() {return "PLAIN";},
                 initialized:        function() { return deferred.promise; },
                 isComplete:         function() {return this._state == "completed";},
                 getPriority:        function() {return 1;},
                 getResponse:        function(challenge)
                                     {
                                         if (this._state == "initial")
                                         {
                                             var responseArray = [0].concat(this._encodeUTF8(challenge.username))
                                                                    .concat([0])
                                                                    .concat(this._encodeUTF8(challenge.password));
                                             var plainResponse = base64.encode(responseArray);
                                             this._state = "generated"
                                             return  {
                                                         mechanism: this.getMechanismName(),
                                                         response: plainResponse
                                                     };
                                         }
                                         else if (this._state == "generated")
                                         {
                                             this._state = "completed";
                                             return null;
                                         }
                                         else
                                         {
                                             throw {message: "Unexpected state '" + this._state +
                                                             ". Cannot handle challenge!"};
                                         }
                                     },
                 toString:           function() { return "[SaslClientPlain]";},
                 getCredentials:     function()
                                     {
                                         return UsernamePasswordProvider.get();
                                     },
                 _encodeUTF8:        function (str)
                                     {
                                         var byteArray = [];
                                         for (var i = 0; i < str.length; i++)
                                         {
                                             if (str.charCodeAt(i) <= 0x7F)
                                             {
                                                 byteArray.push(str.charCodeAt(i));
                                             }
                                             else
                                             {
                                                 var h = encodeURIComponent(str.charAt(i)).substr(1).split('%');
                                                 for (var j = 0; j < h.length; j++)
                                                 {
                                                      byteArray.push(parseInt(h[j], 16));
                                                 }
                                             }
                                         }
                                         return byteArray;
                                     }
            });
       }
);