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
define(["dojo/parser",
        "dojo/query",
        "dojo/_base/connect",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/UpdatableStore",
        "dojox/grid/EnhancedGrid",
        "dijit/registry",
        "dojo/_base/event",
        "dojox/html/entities",
        "dojo/text!showAccessControlProvider.html",
        "dojox/grid/enhanced/plugins/Pagination",
        "dojox/grid/enhanced/plugins/IndirectSelection",
        "dojo/domReady!"],
    function (parser,
              query,
              connect,
              properties,
              updater,
              util,
              UpdatableStore,
              EnhancedGrid,
              registry,
              event,
              entities,
              template)
    {

        function AccessControlProvider(kwArgs)
        {
            this.controller = kwArgs.controller;
            this.modelObj = kwArgs.tabData.modelObject;
            this.management = this.controller.management;
            this.name = this.modelObj.name;
        }

        AccessControlProvider.prototype.getTitle = function ()
        {
            return "AccessControlProvider: " + this.name;
        };

        AccessControlProvider.prototype.open = function (contentPane)
        {
            var that = this;
            this.contentPane = contentPane;

            contentPane.containerNode.innerHTML = template;
            parser.parse(contentPane.containerNode)
                .then(function (instances)
                {
                    that.accessControlProviderUpdater = new AccessControlProviderUpdater(that);
                    var deleteButton = query(".deleteAccessControlProviderButton", contentPane.containerNode)[0];
                    var deleteWidget = registry.byNode(deleteButton);
                    connect.connect(deleteWidget, "onClick", function (evt)
                    {
                        event.stop(evt);
                        that.deleteAccessControlProvider();
                    });
                });
        };

        AccessControlProvider.prototype.close = function ()
        {
            if (this.accessControlProviderUpdater.details)
            {
                this.accessControlProviderUpdater.details.close();
            }
        };

        AccessControlProvider.prototype.deleteAccessControlProvider = function ()
        {
            if (confirm("Are you sure you want to delete access control provider '" + this.name + "'?"))
            {
                var that = this;
                this.controller.management.remove(this.modelObj)
                    .then(function (data)
                    {
                        that.close();
                        that.contentPane.onClose()
                        that.controller.tabContainer.removeChild(that.contentPane);
                        that.contentPane.destroyRecursive();
                    }, util.xhrErrorHandler);
            }
        };

        function AccessControlProviderUpdater(aclTab)
        {
            var node = aclTab.contentPane.containerNode;
            this.contentPane = aclTab.contentPane;
            this.controller = aclTab.controller;
            this.management = this.controller.management;
            this.modelObj = aclTab.modelObj;
            this.name = query(".name", node)[0];
            this.type = query(".type", node)[0];
            this.state = query(".state", node)[0];
            this.priority = query(".priority", node)[0];

            var that = this;

            this.management.load(this.modelObj, {excludeInheritedContext: true})
                .then(function (data)
                {
                    that.accessControlProviderData = data;

                    util.flattenStatistics(that.accessControlProviderData);

                    that.updateHeader();

                    var ui = that.accessControlProviderData.type;
                    require(["qpid/management/accesscontrolprovider/" + ui], function (SpecificProvider)
                    {
                        that.details = new SpecificProvider(query(".providerDetails",
                            node)[0], that.modelObj, that.controller, aclTab);
                    });
                }, function (error)
                {
                    util.tabErrorHandler(error, {
                        updater: that,
                        contentPane: that.contentPane,
                        tabContainer: that.controller.tabContainer,
                        name: that.modelObj.name,
                        category: "Access Control Provider"
                    });
                });
        }

        AccessControlProviderUpdater.prototype.updateHeader = function ()
        {
            this.name.innerHTML = entities.encode(String(this.accessControlProviderData["name"]));
            this.type.innerHTML = entities.encode(String(this.accessControlProviderData["type"]));
            this.state.innerHTML = entities.encode(String(this.accessControlProviderData["state"]));
            this.priority.innerHTML = entities.encode(String(this.accessControlProviderData["priority"]));

        };

        return AccessControlProvider;
    });
