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
        "dojo/_base/array",
        "dojo/_base/event",
        "dojo/dom-construct",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/UpdatableStore",
        "dojox/grid/EnhancedGrid",
        "dijit/registry",
        "dojox/html/entities",
        "dojo/text!showGroupProvider.html",
        "qpid/management/addGroupProvider",
        "dojox/grid/enhanced/plugins/Pagination",
        "dojox/grid/enhanced/plugins/IndirectSelection",
        "dojo/domReady!"],
    function (parser,
              query,
              connect,
              array,
              event,
              construct,
              properties,
              updater,
              util,
              UpdatableStore,
              EnhancedGrid,
              registry,
              entities,
              template,
              addGroupProvider)
    {

        function GroupProvider(kwArgs)
        {
            this.controller = kwArgs.controller;
            this.modelObj = kwArgs.tabData.modelObject;
            this.management = this.controller.management;
            this.name = this.modelObj.name;
        }

        GroupProvider.prototype.getTitle = function ()
        {
            return "GroupProvider: " + this.name;
        };

        GroupProvider.prototype.open = function (contentPane)
        {
            var that = this;
            this.contentPane = contentPane;
            contentPane.containerNode.innerHTML = template;
            parser.parse(contentPane.containerNode)
                .then(function (instances)
                {
                    that.onOpen();
                });
        };

        GroupProvider.prototype.onOpen = function ()
        {
            var that = this;
            this.groupProviderUpdater = new GroupProviderUpdater(this);
            this.groupProviderUpdater.update(function ()
            {
                that._onOpenAfterUpdate();
            });
        };

        GroupProvider.prototype._onOpenAfterUpdate = function ()
        {
            var that = this;
            var contentPane = this.contentPane;
            this.deleteButton = registry.byNode(query(".deleteGroupProviderButton", contentPane.containerNode)[0]);
            this.deleteButton.on("click", function (evt)
            {
                event.stop(evt);
                that.deleteGroupProvider();
            });

            this.editButton = registry.byNode(query(".editGroupProviderButton", contentPane.containerNode)[0]);
            this.editButton.on("click", function (evt)
            {
                event.stop(evt);
                that.editGroupProvider();
            });

            var type = this.groupProviderUpdater.groupProviderData.type;
            var providerDetailsNode = query(".providerDetails", contentPane.containerNode)[0];
            var detailsNode = construct.create("div", null, providerDetailsNode, "last");

            require(["qpid/management/groupprovider/" + encodeURIComponent(type.toLowerCase()) + "/show"],
                function (DetailsUI)
                {
                    that.groupProviderUpdater.details = new DetailsUI({
                        containerNode: detailsNode,
                        parent: that
                    });
                    that.groupProviderUpdater.details.update(that.groupProviderUpdater.groupProviderData);
                });

            var managedInterfaces = this.management.metadata.getMetaData("GroupProvider", type).managedInterfaces;
            if (managedInterfaces)
            {

                var managedInterfaceUI = this.groupProviderUpdater.managedInterfaces;

                array.forEach(managedInterfaces, function (managedInterface)
                {
                    require(["qpid/management/groupprovider/" + encodeURIComponent(managedInterface)],
                        function (ManagedInterface)
                        {
                            managedInterfaceUI[ManagedInterface] =
                                new ManagedInterface(providerDetailsNode, that.modelObj, that.controller);
                            managedInterfaceUI[ManagedInterface].update(that.groupProviderUpdater.groupProviderData);
                        });
                });
            }

            updater.add(this.groupProviderUpdater);
        };

        GroupProvider.prototype.close = function ()
        {
            updater.remove(this.groupProviderUpdater);
        };

        GroupProvider.prototype.deleteGroupProvider = function ()
        {
            var warnMessage = "";
            if (this.groupProviderUpdater.groupProviderData &&
                this.groupProviderUpdater.groupProviderData.type.indexOf("File") !== -1)
            {
                warnMessage = "NOTE: provider deletion will also remove the group file on disk.\n\n";
            }
            if (confirm(warnMessage + "Are you sure you want to delete group provider '" + this.name + "'?"))
            {
                var that = this;
                this.controller.management.remove(this.modelObj)
                    .then(function (data)
                    {
                        that.close();
                        that.contentPane.onClose();
                        that.controller.tabContainer.removeChild(that.contentPane);
                        that.contentPane.destroyRecursive();
                    }, util.xhrErrorHandler);
            }
        };

        GroupProvider.prototype.editGroupProvider = function ()
        {
            addGroupProvider.show(this.controller.management,
                this.modelObj,
                this.groupProviderUpdater.groupProviderData);
        };

        function GroupProviderUpdater(groupProviderTab)
        {
            this.tabObject = groupProviderTab;
            this.contentPane = groupProviderTab.contentPane;
            var controller = groupProviderTab.controller;
            var groupProviderObj = groupProviderTab.modelObj;
            var node = groupProviderTab.contentPane.containerNode;
            this.controller = controller;
            this.management = controller.management;
            this.modelObj = groupProviderObj;
            this.name = query(".name", node)[0];
            this.type = query(".type", node)[0];
            this.state = query(".state", node)[0];
            this.managedInterfaces = {};
            this.details = null;
        }

        GroupProviderUpdater.prototype.updateHeader = function ()
        {
            this.name.innerHTML = entities.encode(String(this.groupProviderData["name"]));
            this.type.innerHTML = entities.encode(String(this.groupProviderData["type"]));
            this.state.innerHTML = entities.encode(String(this.groupProviderData["state"]));
        };

        GroupProviderUpdater.prototype.update = function (callback)
        {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }

            var that = this;
            var management = this.controller.management;
            management.load(this.modelObj, {excludeInheritedContext: true})
                .then(function (data)
                {
                    that._update(data);
                    if (callback)
                    {
                        callback();
                    }
                }, function (error)
                {
                    util.tabErrorHandler(error, {
                        updater: that,
                        contentPane: that.tabObject.contentPane,
                        tabContainer: that.tabObject.controller.tabContainer,
                        name: that.modelObj.name,
                        category: "Group Provider"
                    });
                });
        };

        GroupProviderUpdater.prototype._update = function (data)
        {
            this.groupProviderData = data;
            util.flattenStatistics(this.groupProviderData);
            this.updateHeader();

            if (this.details)
            {
                this.details.update(this.groupProviderData);
            }

            for (var managedInterface in this.managedInterfaces)
            {
                var managedInterfaceUI = this.managedInterfaces[managedInterface];
                if (managedInterfaceUI)
                {
                    managedInterfaceUI.update(this.groupProviderData);
                }
            }
        };

        return GroupProvider;
    });
