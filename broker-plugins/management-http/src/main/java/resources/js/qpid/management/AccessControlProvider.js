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
        "dojo/_base/lang",
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
              lang,
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

                    that.accessControlProviderUpdater.update(function ()
                    {
                        updater.add(that.accessControlProviderUpdater);
                    });
                });
        };

        AccessControlProvider.prototype.close = function ()
        {
           updater.remove(this.accessControlProviderUpdater);
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
            this.providerDetailsDiv = query(".providerDetails", node)[0];
        }

        AccessControlProviderUpdater.prototype.updateHeader = function ()
        {
            this.name.innerHTML = entities.encode(String(this.accessControlProviderData["name"]));
            this.type.innerHTML = entities.encode(String(this.accessControlProviderData["type"]));
            this.state.innerHTML = entities.encode(String(this.accessControlProviderData["state"]));
            this.priority.innerHTML = entities.encode(String(this.accessControlProviderData["priority"]));

        };

        AccessControlProviderUpdater.prototype.update = function (callback) {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }

            this.management.load(this.modelObj)
                .then(lang.hitch(this, function (data) {
                        this._update(data, callback);
                    }),
                    lang.hitch(this, function (error) {
                        util.tabErrorHandler(error, {
                            updater: this,
                            contentPane: this.contentPane,
                            tabContainer: this.controller.tabContainer,
                            name: this.modelObj.name,
                            category: "Access Control Provider"
                        });
                    }));

        };

        AccessControlProviderUpdater.prototype._update = function (data, callback) {
            this.accessControlProviderData = data;
            this.updateHeader();
            if (this.details)
            {
                this.details.update(data);
            }
            else
            {
                require(["qpid/management/accesscontrolprovider/" + data.type],
                    lang.hitch(this, function (SpecificProvider) {
                        this.details = new SpecificProvider(this.providerDetailsDiv,
                            this.modelObj,
                            this.controller);

                        this.details.update(data);

                        if (callback)
                        {
                            callback();
                        }
                    }));
            }
        };

        return AccessControlProvider;
    });
