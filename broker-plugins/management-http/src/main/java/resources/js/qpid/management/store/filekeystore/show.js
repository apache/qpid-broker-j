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

define(["dojo/_base/lang",
        "qpid/common/util",
        "dojo/query",
        "dojo/mouse",
        "dojo/on",
        "dijit/registry",
        "dijit/Tooltip",
        "dijit/form/Button",
        "dojo/domReady!"],
    function (lang, util, query, mouse, on, registry, Tooltip)
{

    function FileKeyStoreProvider(data)
    {
        this.fields = [];
        var attributes = data.parent.management.metadata.getMetaData("KeyStore", "FileKeyStore").attributes;
        for (var name in attributes)
        {
            this.fields.push(name);
        }

        this.parent = data.parent;
        this.management = data.parent.management;
        this.modelObj = data.parent.modelObj;
        this.containerNode = data.containerNode;

        util.buildUI(data.containerNode,
                     data.parent,
                    "store/filekeystore/show.html",
                     this.fields,
                     this,
                     lang.hitch(this, function(){
                         this.reloadButton = registry.byNode(query(".reload", data.containerNode)[0]);
                         this.reloadButton.on("click", lang.hitch(this, this.reload));
                     }));
    }

    FileKeyStoreProvider.prototype.update = function (data)
    {
        util.updateUI(data, this.fields, this);
    };

    FileKeyStoreProvider.prototype.reload = function (evt)
    {
        evt.preventDefault();
        evt.stopPropagation();
        this.reloadButton.set("disabled", true);
        var parentModelObj = this.modelObj;
        var modelObj = {
            type: parentModelObj.type,
            name: "reload",
            parent: parentModelObj
        };
        var url = this.management.buildObjectURL(modelObj);
        this.management.post({url: url}, {})
            .then(lang.hitch(this, function () {
                    this.parent.update();
                    var domNode = this.reloadButton.domNode;
                    Tooltip.show("Keystore is reloaded successfully", domNode);
                    on.once(domNode, mouse.leave, function()
                    {
                        Tooltip.hide(domNode);
                    });
            }),
            this.management.xhrErrorHandler)
            .always(lang.hitch(this, function ()
            {
                this.reloadButton.set("disabled", false);
            }));
    };

    return FileKeyStoreProvider;
});
