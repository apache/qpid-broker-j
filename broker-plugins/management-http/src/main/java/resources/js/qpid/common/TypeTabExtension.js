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
define(["qpid/common/util", "dojo/query", "dojox/html/entities", "dojo/domReady!"], function (util, query, entities)
{

    function TypeTabExtension(containerNode, template, category, type, metadata, data)
    {
        var that = this;
        this.attributeContainers = {};
        if (template)
        {
            util.parse(containerNode, template, function ()
            {
                if (metadata && category && type)
                {
                    var attributes = metadata.getMetaData(category, type).attributes;
                    for (var attrName in attributes)
                    {
                        var queryResult = query("." + attrName, containerNode);
                        if (queryResult && queryResult[0])
                        {
                            var attr = attributes[attrName];
                            that.attributeContainers[attrName] = {
                                containerNode: queryResult[0],
                                attributeType: attr.type
                            };
                        }
                    }
                    that.update(data);
                }
            });
        }
    }

    TypeTabExtension.prototype.update = function (restData)
    {
        for (var attrName in this.attributeContainers)
        {
            if (attrName in restData)
            {
                var content = "";
                if (this.attributeContainers[attrName].attributeType == "Boolean")
                {
                    content = util.buildCheckboxMarkup(restData[attrName]);
                }
                else
                {
                    content = entities.encode(String(restData[attrName]));
                }
                this.attributeContainers[attrName].containerNode.innerHTML = content;
            }
        }
    }

    return TypeTabExtension;
});
