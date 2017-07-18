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
        "qpid/sasl/CredentialBasedSaslClient"],
    function (declare, lang, base64, digestsBase, MD5, SaslClient)
    {
        return declare("qpid.sasl.SaslClientCramMD5", [SaslClient], {
            _state: "initiated",
            getMechanismName: function ()
            {
                return "CRAM-MD5";
            },
            isComplete: function ()
            {
                return this._state == "completed";
            },
            getPriority: function ()
            {
                return 3;
            },
            getInitialResponse: function()
            {
                return null;
            },
            getPassword: function ()
            {
                return this.password;
            },
            getResponse: function (challenge)
            {
                if (this._state == "initiated")
                {
                    var challengeBytes = base64.decode(challenge);
                    var wa = [];
                    var bitLength = challengeBytes.length * 8;
                    for (var i = 0; i < bitLength; i += 8)
                    {
                        wa[i >> 5] |= (challengeBytes[i / 8] & 0xFF) << (i % 32);
                    }
                    var challengeStr = digestsBase.wordToString(wa)
                        .substring(0, challengeBytes.length);

                    var digest = this.username + " " + MD5._hmac(challengeStr,
                            this.getPassword(),
                            digestsBase.outputTypes.Hex);
                    var id = challenge.id;

                    var response = base64.encode(this._encodeUTF8(digest));
                    this._state = "completed";
                    return response;
                }
                else
                {
                    throw new Error("Unexpected state '" + this._state + ". Cannot handle challenge!");
                }
            },
            toString: function ()
            {
                return "[SaslClientCramMD5]";
            },
            _encodeUTF8: function (str)
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
                        var h = encodeURIComponent(str.charAt(i))
                            .substr(1)
                            .split('%');
                        for (var j = 0; j < h.length; j++)
                        {
                            byteArray.push(parseInt(h[j], 16));
                        }
                    }
                }
                return byteArray;
            }
        });
    });
