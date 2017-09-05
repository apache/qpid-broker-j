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
        "dijit/registry",
        "dojo/_base/connect",
        "dojo/_base/event",
        "dojo/json",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/formatter",
        "qpid/common/UpdatableStore",
        "dojox/grid/EnhancedGrid",
        "dojo/data/ObjectStore",
        "qpid/management/group/addGroupMember",
        "dojox/html/entities",
        "dojo/text!group/showGroup.html",
        "dojox/grid/enhanced/plugins/Pagination",
        "dojox/grid/enhanced/plugins/IndirectSelection",
        "dojo/domReady!"],
    function (parser,
              query,
              registry,
              connect,
              event,
              json,
              properties,
              updater,
              util,
              formatter,
              UpdatableStore,
              EnhancedGrid,
              ObjectStore,
              addGroupMember,
              entities,
              showGroup)
    {

        function Group(kwArgs)
        {
            this.controller = kwArgs.controller;
            this.modelObj = kwArgs.tabData.modelObject;
            this.management = this.controller.management;
            this.name = this.modelObj.name;
        }

        Group.prototype.getGroupName = function ()
        {
            return this.name;
        };

        Group.prototype.getGroupProviderName = function ()
        {
            return this.modelObj.parent.name;
        };

        Group.prototype.getTitle = function ()
        {
            return "Group: " + this.name;
        };

        Group.prototype.open = function (contentPane)
        {
            var that = this;
            this.contentPane = contentPane;

            {
                contentPane.containerNode.innerHTML = showGroup;
                parser.parse(contentPane.containerNode)
                    .then(function (instances)
                    {
                        that.groupUpdater = new GroupUpdater(that);
                        that.groupUpdater.update(function ()
                        {
                            var addGroupMemberButton = query(".addGroupMemberButton", contentPane.containerNode)[0];
                            connect.connect(registry.byNode(addGroupMemberButton), "onClick", function (evt)
                            {
                                addGroupMember.show(that.getGroupProviderName(),
                                    that.modelObj,
                                    that.controller.management);
                            });

                            var removeGroupMemberButton = query(".removeGroupMemberButton",
                                contentPane.containerNode)[0];
                            connect.connect(registry.byNode(removeGroupMemberButton), "onClick", function (evt)
                            {
                                util.deleteSelectedObjects(that.groupUpdater.groupMembersUpdatableStore.grid,
                                    "Are you sure you want to remove group member",
                                    that.management,
                                    {
                                        type: "groupmember",
                                        parent: that.modelObj
                                    },
                                    that.groupUpdater);
                            });
                            updater.add(that.groupUpdater);
                        });
                    });
            }
            ;
        };

        Group.prototype.close = function ()
        {
            updater.remove(this.groupUpdater);
        };

        function GroupUpdater(tabObject)
        {
            this.tabObject = tabObject;
            this.contentPane = tabObject.contentPane;
            this.management = tabObject.controller.management;
            this.modelObj = {
                type: "groupmember",
                parent: tabObject.modelObj
            };
            var that = this;
            var containerNode = tabObject.contentPane.containerNode;

            function findNode(name)
            {
                return query("." + name, containerNode)[0];
            }

            function storeNodes(names)
            {
                for (var i = 0; i < names.length; i++)
                {
                    that[names[i]] = findNode(names[i]);
                }
            }

            storeNodes(["name", "state", "durable", "lifetimePolicy", "type"]);
            this.name.innerHTML = entities.encode(String(tabObject.modelObj.name));

            this.groupMemberData = [];

            var gridProperties = {
                keepSelection: true,
                plugins: {
                    pagination: {
                        pageSizes: ["10", "25", "50", "100"],
                        description: true,
                        sizeSwitch: true,
                        pageStepper: true,
                        gotoButton: true,
                        maxPageStep: 4,
                        position: "bottom"
                    },
                    indirectSelection: true

                }
            };

            this.groupMembersUpdatableStore = new UpdatableStore([], findNode("groupMembers"), [{
                name: "Group Member Name",
                field: "name",
                width: "100%"
            }], function (obj)
            {
                connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                {

                });
            }, gridProperties, EnhancedGrid);

        };

        GroupUpdater.prototype.update = function (callback)
        {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }

            var that = this;

            this.management.load(this.modelObj, {excludeInheritedContext: true, depth: 1})
                .then(function (data)
                {
                    that.groupMemberData = data;

                    util.flattenStatistics(that.groupMemberData);

                    if (callback)
                    {
                        callback();
                    }
                    that.groupMembersUpdatableStore.update(that.groupMemberData);
                }, function (error)
                {
                    util.tabErrorHandler(error, {
                        updater: that,
                        contentPane: that.tabObject.contentPane,
                        tabContainer: that.tabObject.controller.tabContainer,
                        name: that.modelObj.name,
                        category: "Group"
                    });
                });
        };

        Group.prototype.deleteGroupMember = function ()
        {
            if (confirm("Are you sure you want to delete group members of group '" + this.name + "'?"))
            {
                this.success = true
                var that = this;
                this.management.remove({
                        type: "groupmember",
                        parent: this.modelObj
                    })
                    .then(function (data)
                    {
                        that.contentPane.onClose()
                        that.controller.tabContainer.removeChild(that.contentPane);
                        that.contentPane.destroyRecursive();
                    }, util.xhrErrorHandler);
            }
        }

        return Group;
    });
