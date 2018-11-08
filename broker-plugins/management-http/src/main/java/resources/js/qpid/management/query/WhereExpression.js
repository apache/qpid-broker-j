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

define(["dojo/_base/declare",
        "dojo/_base/array",
        "dojo/_base/lang",
        "dojo/dom-construct",
        "dojo/Evented",
        "dijit/layout/ContentPane",
        "qpid/management/query/WhereCriteria",
        "dojo/domReady!"], function (declare, array, lang, domConstruct, Evented, ContentPane, WhereCriteria)
{
    return declare("qpid.management.query.WhereExpression", [ContentPane, Evented], {
        whereExpression: "",
        whereFieldsSelector: null,
        _whereItems: null,
        userPreferences: null,

        postCreate: function ()
        {
            this.inherited(arguments);
            this._whereItems = {};
            if (this.whereFieldsSelector)
            {
                this.whereFieldsSelector.on("change", lang.hitch(this, this._whereExpressionChanged));
                var promise = this.whereFieldsSelector.get("selectedItems");
                dojo.when(promise, lang.hitch(this, function (selectedItems)
                {
                    this._whereExpressionChanged(selectedItems);
                }));

            }
        },
        _setWhereFieldsSelectorAttr: function (whereFieldsSelector)
        {
            this.whereFieldsSelector = whereFieldsSelector;
            this.whereFieldsSelector.on("change", lang.hitch(this, this._whereExpressionChanged));
            var promise = this.whereFieldsSelector.get("selectedItems");
            dojo.when(promise, lang.hitch(this, function (items)
            {
                this._whereExpressionChanged(items);
            }));
        },
        _whereExpressionChanged: function (items)
        {
            this._buildWhereCriteriaWidgets(items);
            this._notifyChanged();
        },
        _buildWhereCriteriaWidgets: function (items)
        {
            for (var i = 0; i < items.length; i++)
            {
                var name = items[i].attributeName;
                if (!(name in this._whereItems))
                {
                    this._whereItems[name] = this._createWhereCriteriaWidget(items[i]);
                }
            }
        },
        _createWhereCriteriaWidget: function (item)
        {
            var whereCriteria = new WhereCriteria({
                attributeDetails: item,
                userPreferences: this.userPreferences
            }, domConstruct.create("div"));
            this.addChild(whereCriteria);
            whereCriteria.startup();
            whereCriteria.on("change", lang.hitch(this, this._whereCriteriaChanged));
            return whereCriteria;
        },
        _notifyChanged: function ()
        {
            var expression = this._getWhereExpression();
            this.whereExpression = expression;
            this.emit("change",
                {
                    expression: expression,
                    conditions: this._getConditions()
                });
        },
        _whereCriteriaChanged: function (whereCriteria)
        {
            if (whereCriteria.get("deleted"))
            {
                delete this._whereItems[whereCriteria.get("attributeDetails").attributeName];
                this.removeChild(whereCriteria);
            }
            this._notifyChanged();
        },
        _getWhereExpressionAttr: function ()
        {
            if (!this.whereExpression)
            {
                this.whereExpression = this._getWhereExpression();
            }
            return this.whereExpression;
        },
        _getWhereExpression: function ()
        {
            var columnsAfterChange = [];
            var whereExpression = "";
            var children = this.getChildren();
            var selected = [];
            for (var i = 0; i < children.length; i++)
            {
                if (!children[i].get("deleted"))
                {
                    var details = children[i].get("attributeDetails");
                    columnsAfterChange.push(details);
                    selected.push(details.id);
                    var expression = children[i].getConditionExpression();
                    if (expression)
                    {
                        whereExpression = whereExpression + (whereExpression ? " and " : "") + expression;
                    }
                }
            }
            this.whereFieldsSelector.set("data", {selected: selected});
            return whereExpression;
        },
        _getConditions: function () {
            var conditions = [];
            var children = this.getChildren();
            for (var i = 0; i < children.length; i++)
            {
                if (!children[i].get("deleted"))
                {
                    var childConditions = children[i].getConditions();
                    conditions.push(childConditions);
                }
            }
            return {
                operator: "and",
                conditions: conditions
            };
        },
        clearWhereCriteria: function ()
        {
            this._whereItems = {};
            var children = this.getChildren();
            for (var i = children.length - 1; i >= 0; i--)
            {
                children[i].destroyRecursive(false);
            }
            if (this.whereFieldsSelector)
            {
                this.whereFieldsSelector.set("data", {selected: []});
            }
        },
        _setUserPreferences: function (value)
        {
            this.userPreferences = value;
        }
    });
});