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
 */
define(["dojo/_base/array", "dojox/lang/functional/object"], function (array, fobject)
{
    function Metadata(data)
    {
        this.metadata = data;
    }

    Metadata.prototype.getMetaData = function (category, type)
    {
        if (this.metadata)
        {
            return this.metadata[category][type];
        }
        return null;
    };

    Metadata.prototype.getStatisticsMetadata = function (category, type)
    {
        var metadata = this.getMetaData(category, type);
        if (metadata && metadata.statistics)
        {
            return metadata.statistics;
        }
        else
        {
            return [];
        }
    };

    Metadata.prototype.getDefaultValueForAttribute = function (category, type, attributeName)
    {
        var metaDataForInstance = this.getMetaData(category, type);
        var attributesForType = metaDataForInstance["attributes"];
        var attributesForName = attributesForType[attributeName];
        return attributesForName ? attributesForName["defaultValue"] : undefined;
    };

    Metadata.prototype.isImmutable = function (category, type, attributeName)
    {
        var metaDataForInstance = this.getMetaData(category, type);
        var attributesForType = metaDataForInstance["attributes"];
        var attributesForName = attributesForType[attributeName];
        return attributesForName ? attributesForName["immutable"] : undefined;
    };

    Metadata.prototype.getDefaultValueForType = function (category, type)
    {
        var metaDataForInstance = this.getMetaData(category, type);
        var attributesForType = metaDataForInstance["attributes"];
        var defaultValues = {};
        for (var attributeName in attributesForType)
        {
            if (attributesForType.hasOwnProperty(attributeName))
            {
                var attribute = attributesForType[attributeName];
                if (attribute.defaultValue)
                {
                    if (attribute.type === "Boolean")
                    {
                        defaultValues[attributeName] = (attribute.defaultValue === "true");
                    }
                    else
                    {
                        defaultValues[attributeName] = attribute.defaultValue;
                    }
                }
            }
        }
        return defaultValues;
    };

    Metadata.prototype.getTypesForCategory = function (category)
    {
        return fobject.keys(this.metadata[category]);
    };

    Metadata.prototype.extractUniqueListOfValues = function (data)
    {
        var values = [];
        for (var i = 0; i < data.length; i++)
        {
            for (var j = 0; j < data[i].length; j++)
            {
                var current = data[i][j];
                if (array.indexOf(values, current) === -1)
                {
                    values.push(current);
                }
            }
        }
        return values;
    };

    Metadata.prototype.implementsManagedInterface = function (category, type, managedInterfaceName)
    {
        var md = this.getMetaData(category, type);
        if (md && md.managedInterfaces)
        {
            return array.indexOf(md.managedInterfaces, managedInterfaceName) >= 0;
        }
        return false;
    };

    Metadata.prototype.validChildTypes = function (category, type, childCategory)
    {
        var metaData = this.getMetaData(category, type);
        return metaData ? metaData.validChildTypes[childCategory] : [];
    };

    Metadata.prototype.isCategory = function (category)
    {
        var categoryLower = category ? category.toLowerCase() : null;
        for (var fieldName in this.metadata)
        {
            if (this.metadata.hasOwnProperty(fieldName))
            {
                if (new String(fieldName).toLowerCase() === categoryLower)
                {
                    return true;
                }
            }
        }
        return false;
    };


    return Metadata;
});
