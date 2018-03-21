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
define(["dojo/_base/lang",
        "dojo/dom",
        "dojo/parser",
        "dojo/query",
        "dojo/dom-construct",
        "dijit/registry",
        "dojox/html/entities",
        "dojo/text!accesscontrolprovider/showAclFile.html",
        "dijit/form/Button",
        "dojo/domReady!"],
    function (lang,
              dom,
              parser,
              query,
              construct,
              registry,
              entities,
              template)
    {
        function AclFile(containerNode, aclProviderObj, controller)
        {
            this.modelObj = aclProviderObj;
            this.management = controller.management;
            var node = construct.create("div", null, containerNode, "last");
            node.innerHTML = template;
            parser.parse(containerNode)
                .then(lang.hitch(this, lang.hitch(this, function (instances)
                {
                    this.path = query(".path", node)[0];
                    this.reloadButton = registry.byNode(query(".reload", node)[0]);
                    this.reloadButton.on("click", lang.hitch(this, this.reload));
                })));
        }

        AclFile.prototype.update = function (data)
        {
            this.path.innerHTML = entities.encode(String(data.path));
        };

        AclFile.prototype.reload = function ()
        {
            this.reloadButton.set("disabled", true);
            var parentModelObj = this.modelObj;
            var modelObj = {
                type: parentModelObj.type,
                name: "reload",
                parent: parentModelObj
            };
            var url = this.management.buildObjectURL(modelObj);
            this.management.post({url: url}, {})
                .then(null, management.xhrErrorHandler)
                .always(lang.hitch(this, function ()
                {
                    this.reloadButton.set("disabled", false);
                }));
        };

        return AclFile;
    });
