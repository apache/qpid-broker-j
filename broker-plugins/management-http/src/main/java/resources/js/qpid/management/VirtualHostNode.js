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
        "dijit/registry",
        "dojox/html/entities",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/formatter",
        "qpid/common/UpdatableStore",
        "qpid/management/addQueue",
        "qpid/management/addExchange",
        "qpid/management/editVirtualHostNode",
        "qpid/management/addVirtualHost",
        "dojox/grid/EnhancedGrid",
        "dojo/text!showVirtualHostNode.html",
        "dojo/domReady!"],
    function (parser,
              query,
              lang,
              connect,
              registry,
              entities,
              properties,
              updater,
              util,
              formatter,
              UpdatableStore,
              addQueue,
              addExchange,
              editVirtualHostNode,
              AddVirtualHostDialog,
              EnhancedGrid,
              template)
    {

        function VirtualHostNode(kwArgs)
        {
            this.controller = kwArgs.controller;
            this.modelObj = kwArgs.tabData.modelObject;
            this.management = this.controller.management;
            this.name = this.modelObj.name;
        }

        VirtualHostNode.prototype.getTitle = function ()
        {
            return "VirtualHostNode: " + this.name;
        };

        VirtualHostNode.prototype.open = function (contentPane)
        {
            var that = this;
            this.contentPane = contentPane;
            contentPane.containerNode.innerHTML = template;
            parser.parse(contentPane.containerNode)
                .then(function (instances)
                {
                    that.onOpen(contentPane.containerNode)
                });

        };

        VirtualHostNode.prototype.onOpen = function (containerNode)
        {
            var that = this;
            this.stopNodeButton = registry.byNode(query(".stopNodeButton", containerNode)[0]);
            this.startNodeButton = registry.byNode(query(".startNodeButton", containerNode)[0]);
            this.editNodeButton = registry.byNode(query(".editNodeButton", containerNode)[0]);
            this.deleteNodeButton = registry.byNode(query(".deleteNodeButton", containerNode)[0]);
            this.addVirtualHostButton = registry.byNode(query(".addVHButton", containerNode)[0]);
            this.addVirtualHostButton.on("click", lang.hitch(this, function ()
            {
                var dialog = new AddVirtualHostDialog({
                    management: this.management,
                    virtualhostNodeType: this.vhostNodeUpdater.type.innerHTML,
                    virtualhostNodeModelObject: this.modelObj
                });
                dialog.show();
                dialog.on("done", lang.hitch(this, function (update)
                {
                    dialog.hideAndDestroy();
                    if (update)
                    {
                        this.vhostNodeUpdater.update();
                    }
                }));
            }));

            this.deleteVirtualHostButton = registry.byNode(query(".deleteVHButton", containerNode)[0]);
            this.deleteVirtualHostButton.on("click", lang.hitch(this, function ()
            {
                if (confirm("Deletion of virtual host will delete messages.\n\n"
                            + "Are you sure you want to proceed with delete operation?"))
                {
                    var modeData = this.vhostNodeUpdater.nodeData;
                    if (modeData.virtualhosts)
                    {
                        var modelObj = virtualHostModelObect = {
                            name: modeData.virtualhosts[0].name,
                            type: "virtualhost",
                            parent: this.modelObj
                        };

                        this.management.remove(modelObj)
                            .then(lang.hitch(this, function (result)
                            {
                                this.vhostNodeUpdater.update();
                            }));
                    }
                }
            }));

            this.deleteNodeButton.on("click", function (e)
            {
                if (confirm("Deletion of virtual host node will delete both configuration and message data.\n\n"
                            + "Are you sure you want to delete virtual host node '" + entities.encode(String(that.name))
                            + "'?"))
                {
                    that.management.remove(that.modelObj)
                        .then(function (x)
                        {
                            that.destroy();
                        }, util.xhrErrorHandler);
                }
            });
            this.startNodeButton.on("click", function (event)
            {
                that.startNodeButton.set("disabled", true);
                that.management.update(that.modelObj, {desiredState: "ACTIVE"})
                    .then();
            });

            this.stopNodeButton.on("click", function (event)
            {
                if (confirm("Stopping the node will also shutdown the virtual host. "
                            + "Are you sure you want to stop virtual host node '" + entities.encode(String(that.name))
                            + "'?"))
                {
                    that.stopNodeButton.set("disabled", true);
                    that.management.update(that.modelObj, {desiredState: "STOPPED"})
                        .then();
                }
            });

            this.editNodeButton.on("click", function (event)
            {
                editVirtualHostNode.show(management, that.modelObj, that.vhostNodeUpdater.nodeData);
            });

            this.vhostsGrid = new UpdatableStore([], query(".virtualHost", containerNode)[0], [{
                name: "Name",
                field: "name",
                width: "40%"
            }, {
                name: "State",
                field: "state",
                width: "30%"
            }, {
                name: "Type",
                field: "type",
                width: "30%"
            }], function (obj)
            {
                connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                {
                    var idx = evt.rowIndex, theItem = this.getItem(idx);
                    that.showVirtualHost(theItem);
                });
            }, {
                height: 200,
                canSort: function (col)
                {
                    return false;
                }
            }, EnhancedGrid);

            this.vhostNodeUpdater = new Updater(this);
            this.vhostNodeUpdater.update(function (x)
            {
                updater.add(that.vhostNodeUpdater);
            });
        };

        VirtualHostNode.prototype.showVirtualHost = function (item)
        {
            this.controller.showById(item.id);
        };

        VirtualHostNode.prototype.close = function ()
        {
            updater.remove(this.vhostNodeUpdater);
        };

        VirtualHostNode.prototype.destroy = function ()
        {
            this.close();
            this.contentPane.onClose();
            this.controller.tabContainer.removeChild(this.contentPane);
            this.contentPane.destroyRecursive();
        };

        function Updater(virtualHostNode)
        {
            var domNode = virtualHostNode.contentPane.containerNode;
            this.tabObject = virtualHostNode;
            this.contentPane = virtualHostNode.contentPane;
            this.modelObj = virtualHostNode.modelObj;
            var that = this;

            function findNode(name)
            {
                return query("." + name, domNode)[0];
            }

            function storeNodes(names)
            {
                for (var i = 0; i < names.length; i++)
                {
                    that[names[i]] = findNode(names[i]);
                }
            }

            storeNodes(["name", "state", "type", "defaultVirtualHostNode"]);
            this.detailsDiv = findNode("virtualhostnodedetails");
        }

        Updater.prototype.update = function (callback)
        {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }

            var that = this;
            that.tabObject.management.load(this.modelObj,
                {
                    excludeInheritedContext: true,
                    depth: 1
                })
                .then(function (data)
                {
                    that.nodeData = data || {};
                    that.updateUI(that.nodeData);

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
                        category: "Virtual Host Node"
                    });
                });
        };

        Updater.prototype.updateUI = function (data)
        {
            function showBoolean(val)
            {
                return "<input type='checkbox' disabled='disabled' " + (val ? "checked='checked'" : "") + " />";
            }

            this.tabObject.startNodeButton.set("disabled", !(data.state === "STOPPED" || data.state === "ERRORED"));
            this.tabObject.stopNodeButton.set("disabled", data.state !== "ACTIVE");

            this.name.innerHTML = entities.encode(String(data["name"]));
            this.state.innerHTML = entities.encode(String(data["state"]));
            this.type.innerHTML = entities.encode(String(data["type"]));
            this.defaultVirtualHostNode.innerHTML = showBoolean(data["defaultVirtualHostNode"]);

            if (!this.details)
            {
                var that = this;
                require(["qpid/management/virtualhostnode/" + data.type.toLowerCase() + "/show"],
                    function (VirtualHostNodeDetails)
                    {
                        that.details = new VirtualHostNodeDetails({
                            containerNode: that.detailsDiv,
                            parent: that.tabObject
                        });
                        that.details.update(data);
                    });
            }
            else
            {
                this.details.update(data);
            }

            util.updateUpdatableStore(this.tabObject.vhostsGrid, data.virtualhosts);

            var virtualHostExists = !!data.virtualhosts;
            this.tabObject.addVirtualHostButton.set("disabled",  data.state !== "ACTIVE" || virtualHostExists);
            this.tabObject.deleteVirtualHostButton.set("disabled", data.state !== "ACTIVE" || !virtualHostExists);
        };

        return VirtualHostNode;
    });
