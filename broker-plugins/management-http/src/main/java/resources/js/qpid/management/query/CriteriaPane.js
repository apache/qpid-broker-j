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

define([
  "dojo/_base/declare",
  "dojo/_base/array",
  "dojo/_base/lang",
  "dojo/string",
  "dojo/dom-construct",
  "dojo/dom-style",
  "dojo/text!query/CriteriaPane.html",
  "dojox/html/entities",
  "qpid/common/FormWidgetMixin",
  "dijit/_Widget",
  "dijit/_TemplatedMixin",
  "dijit/_WidgetsInTemplateMixin",
  "dijit/layout/ContentPane",
  "dijit/TitlePane",
  "dijit/form/Button",
  "dijit/form/ValidationTextBox",
  "dijit/form/TextBox",
  "dijit/form/NumberTextBox",
  "dijit/form/Select",
  "dojo/domReady!"
],
function(declare, array, lang, string, domConstruct, domStyle, template, entities)
{
    var ANY = "any";
    var IS_NULL = "is null";
    var IS_NOT_NULL = "is not null";
    var CONTAINS = "contains";
    var NOT_CONTAINS = "not contains";
    var STARTS_WITH = "starts with";
    var ENDS_WIDTH = "ends with";
    var NOT_STARTS_WITH = "not starts with";
    var NOT_ENDS_WIDTH = "not ends with";
    var NOT = "not";
    var EQUAL = "=";
    var NOT_EQUAL = "<>";
    var LESS_THAN = "<";
    var LESS_EQUAL_THAN = "<=";
    var GREATER_THAN = ">";
    var GREATER_EQUAL_THAN = ">=";

    var CONDITIONS_NOT_NEEDING_WIDGET = [ANY,IS_NULL,IS_NOT_NULL];
    var BOOLEAN_CONDITIONS = [ANY,IS_NULL,IS_NOT_NULL,NOT,EQUAL,NOT_EQUAL];
    var STRING_CONDITIONS = [ANY,IS_NULL,IS_NOT_NULL,NOT,EQUAL,NOT_EQUAL, CONTAINS, NOT_CONTAINS, STARTS_WITH, ENDS_WIDTH, NOT_STARTS_WITH, NOT_ENDS_WIDTH];
    var NUMERIC_CONDITIONS = [ANY,IS_NULL,IS_NOT_NULL,NOT,EQUAL,NOT_EQUAL, LESS_THAN, LESS_EQUAL_THAN, GREATER_THAN, GREATER_EQUAL_THAN];
    var ENUM_CONDITIONS = [ANY,IS_NULL,IS_NOT_NULL,NOT,EQUAL,NOT_EQUAL];

    var sqlEscape =                    function(value)
                                       {
                                          return value.replace("'", "'''");
                                       }

    var sqlLikeEscape =                function(value)
                                       {
                                          return sqlEscape(value).replace("%", "\\%").replace("_", "\\_");
                                       }

    var sqlValue =                     function(value, type)
                                       {
                                         return (isNumericType(type) || type === "Boolean") ? value : "'" + sqlEscape(value) + "'"
                                       }

    var isNumericType =                function(type)
                                       {
                                           return (type === "Long"
                                                     || type == "Integer"
                                                     || type == "Double"
                                                     || type == "Float"
                                                     || type == "Short"
                                                     || type == "Number"
                                                     );
                                       }

    var buildExpression =              function(name, condition, value, type)
                                       {
                                            if (condition === ANY)
                                            {
                                                return undefined;
                                            }
                                            else if (condition ===  IS_NULL)
                                            {
                                               return name + " " + IS_NULL;
                                            }
                                            else  if (condition === IS_NOT_NULL)
                                            {
                                               return name + " " + IS_NOT_NULL;
                                            }
                                            else if (condition === CONTAINS || condition === NOT_CONTAINS)
                                            {
                                               var exp = (condition === NOT_CONTAINS ? " " + NOT : "");
                                               return  name + exp + " like '%" + sqlLikeEscape(value) + "%' escape '\\'";
                                            }
                                            else if (condition === STARTS_WITH || condition === NOT_STARTS_WITH)
                                            {
                                               var exp = (condition === NOT_STARTS_WITH ? " " + NOT : "");
                                               return name + exp  + " like '" + sqlLikeEscape(value) + "%' escape '\\'";
                                            }
                                            else if (condition === ENDS_WIDTH  || condition === NOT_ENDS_WIDTH)
                                            {
                                               var exp = (condition === NOT_ENDS_WIDTH ? " " + NOT : "");
                                               return name + exp  + " like '%" + sqlLikeEscape(value) + "' escape '\\'";
                                            }
                                            else if (condition === NOT)
                                            {
                                               return " " + NOT + " " + name + " = " + sqlValue(value, type);
                                            }
                                            else
                                            {
                                                return name + " " + condition + " " + sqlValue(value, type);
                                            }
                                       }

    var getTypeConditions  =           function(type, validValues)
                                       {
                                         if (type === "Boolean")
                                         {
                                            return BOOLEAN_CONDITIONS;
                                         }
                                         else if (isNumericType(type))
                                         {
                                            return NUMERIC_CONDITIONS;
                                         }
                                         else
                                         {
                                            if (validValues && validValues.length)
                                            {
                                                return ENUM_CONDITIONS;
                                            }
                                            return STRING_CONDITIONS;
                                         }
                                       }
    var arrayToOptions =               function(conditions, defaultValue)
                                       {
                                        var options = [];
                                        for (var i=0;i<conditions.length;i++)
                                        {
                                          var selected = conditions[i] == defaultValue;
                                          options.push({label: conditions[i], value:  conditions[i], selected: selected});
                                        }
                                        return options;
                                       }
    var conditionHasWidget =           function(condition)
                                       {
                                         return array.indexOf(CONDITIONS_NOT_NEEDING_WIDGET,condition) == -1;
                                       }


    return declare( "qpid.management.query.CriteriaPane",
                    [dijit._Widget, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, qpid.common.FormWidgetMixin],
                    {
                        //Strip out the apache comment header from the template html as comments unsupported.
                        templateString:    template.replace(/<!--[\s\S]*?-->/g, ""),

                        /**
                         * fields from template
                         */
                        conditionExpression: null,
                        criteriaCondition: null,
                        criteriaValueInputContainer: null,
                        removeButton: null,
                        contentPane: null,
                        titleNode: null,

                        /**
                         * mandatory constructor arguments
                         */
                        criteriaName: null,
                        typeName: null,
                        typeValidValues: null,

                        /**
                         * auxiliary fields
                         */
                         _removed: false,
                         _stored: false,
                         _lastCondition: undefined,
                         _savedValue: undefined,
                         _savedCondition: undefined,

                        postCreate:     function()
                                        {
                                            this.inherited(arguments);
                                            this._postCreate();
                                        },
                        _postCreate:    function()
                                        {
                                            var conditions = getTypeConditions(this.typeName, this.typeValidValues);
                                            this.criteriaCondition.set("options", arrayToOptions(conditions, ANY));
                                            this.criteriaCondition.on("change", lang.hitch(this, this._conditionChanged));


                                            if (this.typeName === "Boolean")
                                            {
                                                this.typeValidValues = ['true', 'false'];
                                            }

                                            var domNode = domConstruct.create("div", {}, this.criteriaValueInputContainer);
                                            if (this.typeValidValues && this.typeValidValues.length)
                                            {
                                                var options = arrayToOptions(this.typeValidValues, this.typeValidValues[0]);
                                                this.valueEditor = new dijit.form.Select({options: options,
                                                                                          disabled: true},
                                                                                          domNode);
                                            }
                                            else
                                            {
                                                this.valueEditor = isNumericType(this.typeName)
                                                                ? new dijit.form.NumberTextBox({
                                                                                                value: 0,
                                                                                                disabled: true,
                                                                                                required:true,
                                                                                                intermediateChanges: true,
                                                                                                invalidMessage:'Please enter a numeric value.'},
                                                                                               domNode)
                                                                : new dijit.form.ValidationTextBox({value: '',
                                                                                                    disabled: true,
                                                                                                    required:true,
                                                                                                    intermediateChanges: true},
                                                                                                    domNode);
                                            }
                                            this.valueEditor.startup();
                                            this.valueEditor.on("change", lang.hitch(this, this._conditionValueChanged));
                                            this.removeCriteria.on("click", lang.hitch(this, this._removalRequested));
                                            this._conditionChanged(ANY);
                                            this._saveState()
                                        },
                        _saveState:     function()
                                        {
                                            this._savedValue = this.valueEditor.value;
                                            this._savedCondition = this.criteriaCondition.value;
                                        },
                        _conditionChanged: function(newValue)
                                        {
                                            if (this.criteriaCondition.value != this._lastCondition)
                                            {
                                                var editable = conditionHasWidget(newValue);
                                                this.valueEditor.set("disabled", !editable);
                                                this._lastCondition = newValue;
                                                this._buildExpression();
                                            }
                                        },
                        _conditionValueChanged: function(newValue)
                                        {
                                            this._buildExpression();
                                        },
                        _buildExpression: function()
                                        {
                                            var expression = buildExpression(this.criteriaName,
                                                                             this.criteriaCondition.value,
                                                                             this._getConditionValue(),
                                                                             this.typeName);
                                            if (!expression)
                                            {
                                              expression = this.criteriaName + ":" + ANY;
                                            }
                                            this.conditionExpression.innerHTML = entities.encode(String(expression));
                                            this.set("value", expression);
                                        },
                        getExpression : function()
                                        {
                                            if (this._removed)
                                            {
                                               return undefined;
                                            }
                                            return buildExpression(this.criteriaName,
                                                                   this.criteriaCondition.value,
                                                                   this._getConditionValue(),
                                                                   this.typeName);
                                        },
                        _getConditionValue: function()
                                        {
                                            return this.valueEditor.value;
                                        },
                        _removalRequested: function()
                                        {
                                            if (this._stored)
                                            {
                                                // do not destroy already stored criteria
                                                // in order to restore it on cancellation

                                                this._removed = true;
                                                domStyle.set(this.domNode, "display", "none");

                                                // signal to listener that criteria is removed
                                                this.set("value", undefined);
                                            }
                                            else
                                            {
                                                this.destroyRecursive(false);
                                            }
                                        },
                        cancelled:      function()
                                        {
                                            if (this._removed)
                                            {
                                                domStyle.set(this.domNode, "display", "");
                                                this._removed = false;
                                            }

                                            if (!this._stored)
                                            {
                                                this.destroyRecursive(false);
                                            }
                                            else
                                            {
                                                 this.valueEditor.set("value", this._savedValue);
                                                 this.criteriaCondition.set("value", this._savedCondition);
                                            }
                                        },
                        submitted:      function()
                                        {
                                            if (this._removed)
                                            {
                                                this.destroyRecursive(false);
                                            }
                                            else
                                            {
                                                this._saveState();
                                                this._stored = true;
                                            }
                                        },
                        setRemovable:   function(removable)
                                        {
                                            this.removeCriteria.set("disabled", !removable);
                                        },
                        validate:       function()
                                        {
                                           if (!this.valueEditor.get("disabled"))
                                           {
                                                return this.valueEditor.isValid();
                                           }
                                           return true;
                                        }

                });

});