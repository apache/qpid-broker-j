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

define(["dojo/query",
        "dojo/_base/lang",
        "qpid/common/util",
        "dojo/domReady!"], function (query, lang, util)
{

    function ManagedCertificateStore(data)
    {
        this.fields = [];
        this.management = data.parent.management;
        this.modelObj = data.parent.modelObj;
        var containerNode = data.containerNode;
        var attributes = this.management.metadata.getMetaData("TrustStore", "ManagedCertificateStore").attributes;
        for (var name in attributes)
        {
            this.fields.push(name);
        }

        util.buildUI(data.containerNode,
            data.parent,
            "store/managedcertificatestore/show.html",
            this.fields,
            this,
            function ()
            {
            });
    }

    ManagedCertificateStore.prototype.update = function (data)
    {
        util.updateUI(data, this.fields, this);
    };

    return ManagedCertificateStore;
});
