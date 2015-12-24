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
        "dojox/encoding/digests/_base",
        "dojox/encoding/digests/MD5",
        "dojo/Deferred",
        "qpid/sasl/SaslClient",
        "qpid/sasl/UsernamePasswordProvider"],
       function(declare, lang, base64, digestsBase, MD5, Deferred, SaslClient, UsernamePasswordProvider)
       {
           var deferred = new Deferred();
           deferred.resolve("initialized");
           return declare("qpid.sasl.SaslClientCramMD5",
                          [SaslClient],
                          {
                              _state:            "initial",
                              initialized:       function() { return deferred.promise;},
                              getMechanismName:  function() {return "CRAM-MD5";},
                              isComplete:        function() {return this._state == "completed";},
                              getPriority:       function() {return 3;},
                              getResponse:       function(data)
                                                 {
                                                    if (this._state == "initial")
                                                    {
                                                      this._initial(data);
                                                      this._state = "initiated";
                                                      return {
                                                                 mechanism: this.getMechanismName()
                                                             };
                                                    }
                                                    else if (this._state == "initiated")
                                                    {
                                                      var challengeBytes = base64.decode(data.challenge);
                                                      var wa=[];
                                                      var bitLength = challengeBytes.length*8;
                                                      for(var i=0; i<bitLength; i+=8)
                                                      {
                                                            wa[i>>5] |= (challengeBytes[i/8] & 0xFF)<<(i%32);
                                                      }
                                                      var challengeStr = digestsBase.wordToString(wa)
                                                                                    .substring(0,challengeBytes.length);

                                                      var digest =  this._username + " " +
                                                                    MD5._hmac(challengeStr, this._password,
                                                                              digestsBase.outputTypes.Hex);
                                                      var id = data.id;

                                                      var response = base64.encode(this._encodeUTF8( digest ));
                                                      this._state = "generated";
                                                      return {
                                                                 id: id,
                                                                 response: response
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
                              toString:           function() { return "[SaslClientCramMD5]";},
                              getCredentials:     function()
                                                  {
                                                      return UsernamePasswordProvider.get();
                                                  },
                              _initial   :        function(data)
                                                  {
                                                     this._password = data.password;
                                                     this._username = data.username;
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