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
define(["dojo/_base/connect",
        "dojox/html/entities",
        "dojo/query",
        "dojo/json",
        "dijit/registry",
        "dojox/grid/EnhancedGrid",
        "qpid/common/UpdatableStore",
        "qpid/common/util",
        "dojo/domReady!"], function (connect, entities, query, json, registry, EnhancedGrid, UpdatableStore, util)
{
    var priorityNames = {
        '_0': 'Never',
        '_1': 'Default',
        '_2': 'High',
        '_3': 'Highest'
    };
    var nodeFields = ["storePath", "groupName", "role", "address", "designatedPrimary", "priority", "quorumOverride"];

    function findNode(nodeClass, containerNode)
    {
        return query("." + nodeClass, containerNode)[0];
    }

    function getModelObj(nodeName, remoteNodeName, modelObj)
    {
        if (nodeName == remoteNodeName)
        {
            return modelObj;
        }
        else
        {
            return {
                name: remoteNodeName,
                type: "remotereplicationnode",
                parent: modelObj
            };
        }
    }

    function BDBHA(data)
    {
        this.parent = data.parent;
        var that = this;
        util.buildUI(data.containerNode, data.parent, "virtualhostnode/bdb_ha/show.html", nodeFields, this, function ()
        {
            that._postParse(data);
        });
    };
    BDBHA.prototype._postParse = function (data)
    {
        var that = this;
        var containerNode = data.containerNode;
        this.management = data.parent.management;
        this.designatedPrimaryContainer = findNode("designatedPrimaryContainer", containerNode);
        this.priorityContainer = findNode("priorityContainer", containerNode);
        this.quorumOverrideContainer = findNode("quorumOverrideContainer", containerNode);
        this.permittedNodes = query(".permittedNodes", containerNode)[0];
        this.membersGridPanel = registry.byNode(query(".membersGridPanel", containerNode)[0]);
        this.membersGrid = new UpdatableStore([], findNode("groupMembers", containerNode), [{
            name: 'Name',
            field: 'name',
            width: '10%'
        }, {
            name: 'Role',
            field: 'role',
            width: '15%'
        }, {
            name: 'Address',
            field: 'address',
            width: '30%'
        }, {
            name: 'Join Time',
            field: 'joinTime',
            width: '25%',
            formatter: function (value)
            {
                return value ? that.management.userPreferences.formatDateTime(value) : "";
            }
        }, {
            name: 'Replication Transaction ID',
            field: 'lastKnownReplicationTransactionId',
            width: '20%',
            formatter: function (value)
            {
                return value > 0 ? value : "N/A";
            }
        }], null, {
            selectionMode: "single",
            keepSelection: true,
            plugins: {
                indirectSelection: true
            }
        }, EnhancedGrid, true);

        this.removeNodeButton = registry.byNode(query(".removeNodeButton", containerNode)[0]);
        this.transferMasterButton = registry.byNode(query(".transferMasterButton", containerNode)[0]);
        this.transferMasterButton.set("disabled", true);
        this.removeNodeButton.set("disabled", true);

        var nodeControlsToggler = function (rowIndex)
        {
            var data = that.membersGrid.grid.selection.getSelected();
            that.transferMasterButton.set("disabled", data.length != 1 || data[0].role != "REPLICA");
            that.removeNodeButton.set("disabled", data.length != 1 || data[0].role == "MASTER");
        };
        connect.connect(this.membersGrid.grid.selection, 'onSelected', nodeControlsToggler);
        connect.connect(this.membersGrid.grid.selection, 'onDeselected', nodeControlsToggler);

        var modelObj = data.parent.modelObj;
        this.transferMasterButton.on("click", function (e)
        {
            var data = that.membersGrid.grid.selection.getSelected();
            if (data.length == 1 && confirm("Are you sure you would like to transfer mastership to node '"
                                            + data[0].name + "'?"))
            {
                that.management.update(getModelObj(that.data.name, data[0].name, modelObj), {role: "MASTER"})
                    .then(function (data)
                    {
                        that.membersGrid.grid.selection.clear();
                    });
            }
        });

        this.removeNodeButton.on("click", function (e)
        {
            var data = that.membersGrid.grid.selection.getSelected();
            if (data.length == 1 && confirm("Are you sure you would like to delete node '" + data[0].name + "'?"))
            {
                that.management.remove(getModelObj(that.data.name, data[0].name, modelObj))
                    .then(function (data)
                    {
                        that.membersGrid.grid.selection.clear();
                        if (data[0].name == that.data.name)
                        {
                            that.parent.destroy();
                        }
                    }, util.xhrErrorHandler);
            }
        });
        this._parsed = true;
    }

    BDBHA.prototype.update = function (data)
    {
        if (!this._parsed)
        {
            return;
        }

        this.parent.editNodeButton.set("disabled", false);

        var permittedNodesMarkup = "";
        if (data.permittedNodes)
        {
            for (var i = 0; i < data.permittedNodes.length; i++)
            {
                permittedNodesMarkup += "<div>" + data.permittedNodes[i] + "</div>";
            }
        }
        this.permittedNodes.innerHTML = permittedNodesMarkup;

        this.data = data;
        for (var i = 0; i < nodeFields.length; i++)
        {
            var name = nodeFields[i];
            if (name == "priority")
            {
                this[name].innerHTML = priorityNames["_" + data[name]];
            }
            else if (name == "quorumOverride")
            {
                this[name].innerHTML = (data[name] == 0 ? "MAJORITY" : entities.encode(String(data[name])));
            }
            else
            {
                this[name].innerHTML = entities.encode(String(data[name]));
            }
        }

        var members = data.remotereplicationnodes;
        if (members)
        {
            members.push({
                id: data.id,
                name: data.name,
                groupName: data.groupName,
                address: data.address,
                role: data.role,
                joinTime: data.joinTime,
                lastKnownReplicationTransactionId: data.lastKnownReplicationTransactionId
            });
        }
        this._updateGrid(members, this.membersGridPanel, this.membersGrid);

        if (!members || members.length < 3)
        {
            this.designatedPrimaryContainer.style.display = "block";
            this.quorumOverrideContainer.style.display = "none";
        }
        else
        {
            this.designatedPrimaryContainer.style.display = "none";
            this.quorumOverrideContainer.style.display = "block";
        }
    };

    BDBHA.prototype._updateGrid = function (conf, panel, updatableGrid)
    {
        if (conf && conf.length > 0)
        {
            panel.domNode.style.display = "block";
            var changed = updatableGrid.update(conf);
            if (changed)
            {
                updatableGrid.grid._refresh();
            }
        }
        else
        {
            panel.domNode.style.display = "none";
        }
    }

    return BDBHA;
});
