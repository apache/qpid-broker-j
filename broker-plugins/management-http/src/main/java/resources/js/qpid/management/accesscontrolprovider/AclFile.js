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
define(["dojo/_base/xhr",
        "dojo/dom",
        "dojo/parser",
        "dojo/query",
        "dojo/dom-construct",
        "dojo/_base/connect",
        "dojo/_base/window",
        "dojo/_base/event",
        "dojo/_base/json",
        "dojo/_base/lang",
        "dijit/registry",
        "dojox/html/entities",
        "qpid/common/util",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/UpdatableStore",
        "dojox/grid/EnhancedGrid",
        "dojox/grid/enhanced/plugins/Pagination",
        "dojox/grid/enhanced/plugins/IndirectSelection",
        "dojox/validate/us",
        "dojox/validate/web",
        "dijit/Dialog",
        "dijit/form/TextBox",
        "dijit/form/ValidationTextBox",
        "dijit/form/TimeTextBox",
        "dijit/form/Button",
        "dijit/form/Form",
        "dijit/form/DateTextBox",
        "dojo/domReady!"],
    function (xhr,
              dom,
              parser,
              query,
              construct,
              connect,
              win,
              event,
              json,
              lang,
              registry,
              entities,
              util,
              properties,
              updater,
              UpdatableStore,
              EnhancedGrid)
    {
        function AclFile(containerNode, aclProviderObj, controller, tabObject)
        {
            var node = construct.create("div", null, containerNode, "last");
            this.modelObj = aclProviderObj;
            var that = this;
            this.name = aclProviderObj.name;
            xhr.get({
                url: "accesscontrolprovider/showAclFile.html",
                sync: true,
                load: function (data)
                {
                    node.innerHTML = data;
                    parser.parse(node)
                        .then(function (instances)
                        {
                            that.groupDatabaseUpdater = new AclFileUpdater(node, tabObject);

                            updater.add(that.groupDatabaseUpdater);

                            that.groupDatabaseUpdater.update();
                        });

                }
            });
        }

        AclFile.prototype.close = function ()
        {
            updater.remove(this.groupDatabaseUpdater);
        };

        function AclFileUpdater(node, tabObject)
        {
            this.tabObject = tabObject;
            this.contentPane = tabObject.contentPane;
            var aclProviderObj = tabObject.modelObj;
            var controller = tabObject.controller;
            this.controller = controller;
            this.modelObj = aclProviderObj;
            this.management = controller.management;
            this.name = aclProviderObj.name;
            this.path = query(".path", node)[0];
            this.reloadButton = registry.byNode(query(".reload", node)[0]);
            this.reloadButton.on("click", lang.hitch(this, this.reload));
        }

        AclFileUpdater.prototype.update = function ()
        {
            if (!this.contentPane.selected)
            {
                return;
            }

            var that = this;

            this.management.load(this.modelObj, {excludeInheritedContext: true})
                .then(function (data)
                {
                    if (data[0])
                    {
                        that.aclProviderData = data[0];
                        that.path.innerHTML = entities.encode(String(that.aclProviderData.path));
                    }
                }, function (error)
                {
                    util.tabErrorHandler(error, {
                        updater: that,
                        contentPane: that.tabObject.contentPane,
                        tabContainer: that.tabObject.controller.tabContainer,
                        name: that.modelObj.name,
                        category: "Access Control Provider"
                    });
                });

        };

        AclFileUpdater.prototype.reload = function ()
        {
            var parentModelObj = this.modelObj;
            var modelObj = {
                type: parentModelObj.type,
                name: "reload",
                parent: parentModelObj
            };
            var url = this.management.buildObjectURL(modelObj);
            this.management.post({url: url}, {});
        };

        return AclFile;
    });
