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
        "qpid/sasl/cram-md5/SaslClient"], function (declare, lang, base64, digestsBase, MD5, SaslClientCramMD5)
{
    return declare("qpid.sasl.SaslClientCramMD5Hex", [SaslClientCramMD5], {
        getMechanismName: function ()
        {
            return "CRAM-MD5-HEX";
        },
        getPriority: function ()
        {
            return 2;
        },
        toString: function ()
        {
            return "[SaslClientCramMD5Hex]";
        },
        getPassword: function ()
        {
            return MD5(this.password, digestsBase.outputTypes.Hex);
        }
    });
});
