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
 *
 */

define(["dojo/_base/lang"],
    function (lang)
    {
        var traverseStructure = function traverseTree(structure, parent, visit)
        {
            var result = visit(parent);
            if (result)
            {
                return result;
            }
            for (var fieldName in structure)
            {
                var fieldValue = structure[fieldName];
                if (lang.isArray(fieldValue))
                {
                    var fieldType = fieldName.substring(0, fieldName.length - 1);
                    for (var i = 0; i < fieldValue.length; i++)
                    {
                        var object = fieldValue[i];
                        var item = {
                            id: object.id,
                            name: object.name,
                            type: fieldType,
                            parent: parent
                        };
                        result = traverseStructure(object, item, visit);
                        if (result)
                        {
                            return result;
                        }
                    }
                }
            }
            return false;
        };

        var findObjectById = function findObjectById(structureRoot, id)
        {
            return traverseStructure(
                structureRoot,
                {
                    id: structureRoot.id,
                    name: structureRoot.name,
                    type: "broker"
                },
                function (item)
                {
                    if (item.id === id)
                    {
                        return item;
                    }
                });
        };

        var findObjectsByType = function findObjectsByType(structureRoot, type)
        {
            var items = [];
            traverseStructure(
                structureRoot,
                {
                    id: structureRoot.id,
                    name: structureRoot.name,
                    type: "broker"
                },
                function (item)
                {
                    if (item.type === type)
                    {
                        items.push(item);
                    }
                    return false;
                });
            return items;
        };

        function Structure()
        {
            this.structure = null;
        }

        Structure.prototype.update = function (structure)
        {
            this.structure = structure;
        };

        Structure.prototype.findById = function (id)
        {
            return findObjectById(this.structure, id);
        };

        Structure.prototype.findByType = function (type)
        {
            return findObjectsByType(this.structure, type);
        };

        Structure.prototype.getScopeItems = function ()
        {
            var brokers = this.findByType("broker");
            var virtualHosts = this.findByType("virtualhost");
            var objects = brokers.concat(virtualHosts);

            var items = [];
            var scopeModelObjects = {};
            for (var i = 0; i < objects.length; i++)
            {
                if (objects[i].type === "broker")
                {
                    name = objects[i].name;
                    brokerId = objects[i].id;
                }
                else
                {
                    name = "VH:" + objects[i].parent.name + "/" + objects[i].name;
                }
                var id = objects[i].id;
                items.push({
                    id: id,
                    name: name
                });
                scopeModelObjects[id] = objects[i];
            }

            return {
                scopeModelObjects: scopeModelObjects,
                items: items
            };
        };

        Structure.prototype.getHierarchicalName = function (object)
        {
            if (object && object.parent)
            {
                var type = object.type.charAt(0).toUpperCase() + object.type.substring(1);
                var val = object.name;
                for (var i = object.parent; i && i.parent; i = i.parent)
                {
                    val = i.name + "/" + val;
                }
                return " (" + type + ":" + val + ")" ;
            }
            return "";
        };

        return Structure;
    });
