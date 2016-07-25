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
        "dojo/query",
        "dijit/Tree",
        "qpid/common/util",
        "qpid/management/controller",
        "dojo/ready",
        "dojo/domReady!"], function (lang, query, Tree, util, controller)
{

    function TreeViewModel(management, node)
    {
        this.management = management;
        this.node = node;

        this.onChildrenChange = function (parent, children)
        {
            // fired when the set of children for an object change
        };

        this.onChange = function (object)
        {
            // fired when the properties of an object change
        };

        this.onDelete = function (object)
        {
            // fired when an object is deleted
        };
    }

    TreeViewModel.prototype.buildModel = function (data)
    {
        this.model = data;

    };

    TreeViewModel.prototype.updateModel = function (data)
    {
        var that = this;

        function checkForChanges(oldData, data)
        {
            var propName;
            if (oldData.name != data.name)
            {
                that.onChange(data);
            }

            var childChanges = false;
            // Iterate over old childTypes, check all are in new
            for (propName in oldData)
            {
                if (oldData.hasOwnProperty(propName))
                {
                    var oldChildren = oldData[propName];
                    if (util.isArray(oldChildren))
                    {

                        var newChildren = data[propName];

                        if (!(newChildren && util.isArray(newChildren)))
                        {
                            childChanges = true;
                        }
                        else
                        {
                            var subChanges = false;
                            // iterate over elements in array, make sure in both, in which case recurse
                            for (var i = 0; i < oldChildren.length; i++)
                            {
                                var matched = false;
                                for (var j = 0; j < newChildren.length; j++)
                                {
                                    if (oldChildren[i].id == newChildren[j].id)
                                    {
                                        checkForChanges(oldChildren[i], newChildren[j]);
                                        matched = true;
                                        break;
                                    }
                                }
                                if (!matched)
                                {
                                    subChanges = true;
                                }
                            }
                            if (subChanges == true || oldChildren.length != newChildren.length)
                            {
                                that.onChildrenChange({
                                    id: data.id + propName,
                                    _dummyChild: propName,
                                    data: data
                                }, newChildren);
                            }
                        }
                    }
                }
            }

            for (propName in data)
            {
                if (data.hasOwnProperty(propName))
                {
                    var prop = data[propName];
                    if (util.isArray(prop))
                    {
                        if (!(oldData[propName] && util.isArray(oldData[propName])))
                        {
                            childChanges = true;
                        }
                    }
                }
            }

            if (childChanges)
            {
                var children = [];
                that.getChildren(data, function (theChildren)
                {
                    children = theChildren
                });
                that.onChildrenChange(data, children);
            }
        }

        var oldData = this.model;
        this.model = data;

        checkForChanges(oldData, data);
    };

    TreeViewModel.prototype.fetchItemByIdentity = function (id)
    {

        function fetchItem(id, data)
        {
            var propName;

            if (data.id == id)
            {
                return data;
            }
            else if (id.indexOf(data.id) == 0)
            {
                return {
                    id: id,
                    _dummyChild: id.substring(id.length),
                    data: data
                };
            }
            else
            {
                for (propName in data)
                {
                    if (data.hasOwnProperty(propName))
                    {
                        var prop = data[propName];
                        if (util.isArray(prop))
                        {
                            for (var i = 0; i < prop.length; i++)
                            {
                                var theItem = fetchItem(id, prop[i]);
                                if (theItem)
                                {
                                    return theItem;
                                }
                            }
                        }
                    }
                }
                return null;
            }
        }

        return fetchItem(id, this.model);
    };

    TreeViewModel.prototype.getChildren = function (parentItem, onComplete)
    {

        if (parentItem)
        {
            if (parentItem._dummyChild)
            {
                onComplete(parentItem.data[parentItem._dummyChild]);
            }
            else
            {
                var children = [];
                for (var propName in parentItem)
                {
                    if (parentItem.hasOwnProperty(propName))
                    {
                        var prop = parentItem[propName];

                        if (util.isArray(prop))
                        {
                            children.push({
                                id: parentItem.id + propName,
                                _dummyChild: propName,
                                data: parentItem
                            });
                        }
                    }
                }
                onComplete(children);
            }
        }
        else
        {
            onComplete([]);
        }
    };

    TreeViewModel.prototype.getIdentity = function (theItem)
    {
        if (theItem)
        {
            return theItem.id;
        }

    };

    TreeViewModel.prototype.getLabel = function (theItem)
    {
        if (theItem)
        {
            if (theItem._dummyChild)
            {
                return theItem._dummyChild;
            }
            else
            {
                return theItem.name;
            }
        }
        else
        {
            return "";
        }
    };

    TreeViewModel.prototype.getRoot = function (onItem)
    {
        onItem(this.model);
    };

    TreeViewModel.prototype.mayHaveChildren = function (theItem)
    {
        if (theItem)
        {
            if (theItem._dummyChild)
            {
                return true;
            }
            else
            {
                for (var propName in theItem)
                {
                    if (theItem.hasOwnProperty(propName))
                    {
                        var prop = theItem[propName];
                        if (util.isArray(prop))
                        {
                            return true;
                        }
                    }
                }
                return false;
            }
        }
        else
        {
            return false;
        }
    };

    TreeViewModel.prototype.relocate = function (theItem)
    {
        controller.showById(theItem.id);
    };

    TreeViewModel.prototype.update = function (data)
    {
        if (this.model)
        {
            this.updateModel(data);
        }
        else
        {
            this.buildModel(data);
            this.startUp();
        }
    };

    TreeViewModel.prototype.startUp = function ()
    {
        var tree = new Tree({model: this}, this.node);
        tree.on("dblclick", lang.hitch(this, function (object)
        {
            if (object && !object._dummyChild)
            {
                this.relocate(object);
            }

        }), true);
        tree.startup();
    };

    return TreeViewModel;
});