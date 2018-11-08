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
        "dojo/string",
        "dojo/dom-construct",
        "dojo/dom-style",
        "dojo/sniff",
        "dojo/text!query/CriteriaPane.html",
        "dojox/html/entities",
        "dojo/Evented",
        "dojo/number",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dijit/layout/ContentPane",
        "dijit/TitlePane",
        "dijit/form/Button",
        "dijit/form/ValidationTextBox",
        "dijit/form/TextBox",
        "dijit/form/NumberTextBox",
        "dijit/form/MultiSelect",
        "dijit/form/TimeTextBox",
        "dijit/form/DateTextBox",
        "dijit/form/NumberSpinner",
        "dojo/domReady!"],
    function (declare, array, lang, string, domConstruct, domStyle, has, template, entities, Evented, number)
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
        var IN = "in"
        var NOT_IN = "not in"

        var CONDITIONS_NOT_NEEDING_WIDGET = [ANY, IS_NULL, IS_NOT_NULL];
        var BOOLEAN_CONDITIONS = [ANY, IS_NULL, IS_NOT_NULL, EQUAL, NOT_EQUAL];
        var STRING_CONDITIONS = [ANY,
                                 IS_NULL,
                                 IS_NOT_NULL,
                                 EQUAL,
                                 NOT_EQUAL,
                                 CONTAINS,
                                 NOT_CONTAINS,
                                 STARTS_WITH,
                                 ENDS_WIDTH,
                                 NOT_STARTS_WITH,
                                 NOT_ENDS_WIDTH];
        var NUMERIC_CONDITIONS = [ANY,
                                  IS_NULL,
                                  IS_NOT_NULL,
                                  EQUAL,
                                  NOT_EQUAL,
                                  LESS_THAN,
                                  LESS_EQUAL_THAN,
                                  GREATER_THAN,
                                  GREATER_EQUAL_THAN];
        var ENUM_CONDITIONS = [ANY, IS_NULL, IS_NOT_NULL, IN, NOT_IN];
        var DATE_CONDITIONS = [ANY,
                               IS_NULL,
                               IS_NOT_NULL,
                               EQUAL,
                               NOT_EQUAL,
                               LESS_THAN,
                               LESS_EQUAL_THAN,
                               GREATER_THAN,
                               GREATER_EQUAL_THAN];
        var UNSUPPORTED_TYPES_CONDITIONS = [ANY, IS_NULL, IS_NOT_NULL];
        var UNSUPPORTED_TYPES = ["Map", "Collection", "Set", "List"];

        var sqlEscape = function (value)
        {
            return value.replace(/'/g, "'''");
        }

        var sqlLikeEscape = function (value)
        {
            return sqlEscape(value)
                .replace(/%/g, "\\%")
                .replace(/_/g, "\\_");
        }

        var sqlValue = function (value, type)
        {
            return (isNumericType(type) || type === "Boolean" || type === "Date") ? value : "'" + sqlEscape(value) + "'"
        }

        var isNumericType = function (type)
        {
            return (type === "Long" || type == "Integer" || type == "Double" || type == "Float" || type == "Short"
                    || type == "Number"
            );
        }

        var buildExpression = function (name, condition, value, type)
        {
            if (condition === ANY)
            {
                return undefined;
            }
            else if (condition === IS_NULL)
            {
                return name + " " + IS_NULL;
            }
            else if (condition === IS_NOT_NULL)
            {
                return name + " " + IS_NOT_NULL;
            }
            else if (condition === CONTAINS || condition === NOT_CONTAINS)
            {
                var exp = (condition === NOT_CONTAINS ? " " + NOT : "");
                return name + exp + " like '%" + sqlLikeEscape(value) + "%' escape '\\'";
            }
            else if (condition === STARTS_WITH || condition === NOT_STARTS_WITH)
            {
                var exp = (condition === NOT_STARTS_WITH ? " " + NOT : "");
                return name + exp + " like '" + sqlLikeEscape(value) + "%' escape '\\'";
            }
            else if (condition === ENDS_WIDTH || condition === NOT_ENDS_WIDTH)
            {
                var exp = (condition === NOT_ENDS_WIDTH ? " " + NOT : "");
                return name + exp + " like '%" + sqlLikeEscape(value) + "' escape '\\'";
            }
            else if (condition === IN || condition === NOT_IN)
            {
                if (!value)
                {
                    return undefined;
                }

                var exp = condition + " (";
                if (lang.isArray(value))
                {
                    if (!value.length)
                    {
                        return undefined;
                    }
                    for (var i = 0; i < value.length; i++)
                    {
                        if (i > 0)
                        {
                            exp = exp + ",";
                        }
                        exp = exp + sqlValue(value[i], type);
                    }
                }
                else
                {
                    exp = exp + sqlValue(value, type);
                }
                exp = exp + " )";
                return name + " " + exp;
            }
            else
            {
                return name + " " + condition + " " + sqlValue(value, type);
            }
        }

        var getTypeConditions = function (type, validValues)
        {
            if (type === "Boolean")
            {
                return BOOLEAN_CONDITIONS;
            }
            if (type === "Date")
            {
                return DATE_CONDITIONS;
            }
            else if (isNumericType(type))
            {
                return NUMERIC_CONDITIONS;
            }
            else
            {
                if (UNSUPPORTED_TYPES.indexOf(type) != -1)
                {
                    return UNSUPPORTED_TYPES_CONDITIONS;
                }

                if (validValues && validValues.length)
                {
                    return ENUM_CONDITIONS;
                }
                else
                {
                    return STRING_CONDITIONS;
                }
            }
        }

        var arrayToOptions = function (conditions, defaultValue)
        {
            var options = [];
            for (var i = 0; i < conditions.length; i++)
            {
                var selected = conditions[i] == defaultValue;
                options.push({
                    label: conditions[i],
                    value: conditions[i],
                    selected: selected
                });
            }
            return options;
        }
        var conditionHasWidget = function (condition)
        {
            return array.indexOf(CONDITIONS_NOT_NEEDING_WIDGET, condition) == -1;
        }
        var isInt = function isInt(value)
        {
            return !isNaN(value) && parseInt(Number(value)) == value && !isNaN(parseInt(value, 10));
        }

        // dojo TimeTextBox has a bug in Firefox
        // https://bugzilla.mozilla.org/show_bug.cgi?id=487897
        // Custom TimePicker is implemented for Firefox
        var TimePicker = declare("qpid.management.TimePicker", [dijit._WidgetBase, Evented], {
            disabled: false,
            intermediateChanges: false,
            value: undefined,
            buildRendering: function ()
            {
                var domNode = this.domNode = domConstruct.create("div", {className: "dijitReset dijitInline"});
                domConstruct.create("span", {innerHTML: "T"}, domNode);
                var timeNode = domConstruct.create("div", {}, domNode);
                this.hoursEditor = new dijit.form.NumberSpinner({
                    name: "hours",
                    constraints: {
                        max: 23,
                        min: 0
                    },
                    style: {width: "4em"},
                    intermediateChanges: this.intermediateChanges,
                    title: "Hours in range 0-23"
                }, timeNode);
                this.hoursEditor.on("change", lang.hitch(this, this._setValue));
                domConstruct.create("span", {innerHTML: ":"}, domNode);
                var minutesNode = domConstruct.create("div", {}, domNode);
                this.minutesEditor = new dijit.form.NumberSpinner({
                    name: "minutes",
                    constraints: {
                        max: 59,
                        min: 0
                    },
                    style: {width: "4em"},
                    intermediateChanges: this.intermediateChanges,
                    title: "Minutes in range 0-59"
                }, minutesNode);
                this.minutesEditor.on("change", lang.hitch(this, this._setValue));
                domConstruct.create("span", {innerHTML: ":"}, domNode);
                var secondsNode = domConstruct.create("div", {}, domNode);
                this.secondsEditor = new dijit.form.NumberSpinner({
                    name: "seconds",
                    constraints: {
                        max: 59,
                        min: 0
                    },
                    style: {width: "4em"},
                    intermediateChanges: this.intermediateChanges,
                    title: "Seconds in range 0-59"
                }, secondsNode);
                this.secondsEditor.on("change", lang.hitch(this, this._setValue));
                domConstruct.create("span", {innerHTML: "."}, domNode);
                var millisecondsNode = domConstruct.create("div", {}, domNode);
                this.millisecondsEditor = new dijit.form.NumberSpinner({
                    name: "milliseconds",
                    constraints: {
                        max: 999,
                        min: 0
                    },
                    style: {width: "5em"},
                    intermediateChanges: this.intermediateChanges,
                    title: "Milliseconds in range 0-999"
                }, millisecondsNode);
                this.millisecondsEditor.on("change", lang.hitch(this, this._setValue));
            },
            startup: function ()
            {
                this.inherited(arguments);
                this.hoursEditor.startup();
                this.minutesEditor.startup();
                this.secondsEditor.startup();
                this.millisecondsEditor.startup();
            },
            _setValue: function ()
            {
                var time = 0
                if (isInt(this.hoursEditor.value))
                {
                    time = time + this.hoursEditor.value * 60 * 60 * 1000;
                }
                if (isInt(this.minutesEditor.value))
                {
                    time = time + this.minutesEditor.value * 60 * 1000;
                }
                if (isInt(this.secondsEditor.value))
                {
                    time = time + this.secondsEditor.value * 1000;
                }
                if (isInt(this.millisecondsEditor.value))
                {
                    time = time + this.millisecondsEditor.value;
                }
                this.value = new Date(time - new Date().getTimezoneOffset() * 60 * 1000);
                this.emit("change", this.value);
            },
            _setDisabledAttr: function (value)
            {
                this.inherited(arguments);
                this.disabled = value;
                this.hoursEditor.set("disabled", value);
                this.minutesEditor.set("disabled", value);
                this.secondsEditor.set("disabled", value);
                this.millisecondsEditor.set("disabled", value);
            },
            _setValueAttr: function (value)
            {
                if (value)
                {
                    var date = value instanceof Date ? value : new Date(value);
                    this.hoursEditor.set("value", date.getHours());
                    this.minutesEditor.set("value", date.getMinutes());
                    this.secondsEditor.set("value", date.getSeconds());
                    this.millisecondsEditor.set("value", date.getMilliseconds());
                }
                this.value = date;
                this.inherited(arguments);
            },
            _getValueAttr: function ()
            {
                return this.value;
            }
        });

        var DateTimePicker = declare("qpid.management.DateTimePicker", [dijit._WidgetBase, Evented], {
            disabled: false,
            value: undefined,
            buildRendering: function ()
            {
                var domNode = this.domNode = domConstruct.create("div", {className: "dijitReset dijitInline"});
                var dateNode = domConstruct.create("div", {}, domNode);
                this.dateEditor = new dijit.form.DateTextBox({
                    name: "date",
                    intermediateChanges: true,
                    constraints: {datePattern: "yyyy-MM-dd"}
                }, dateNode);
                this.dateEditor.on("change", lang.hitch(this, this._setValue));
                var timeNode = domConstruct.create("div", {}, domNode);
                if (has("ff"))
                {
                    this.timeEditor = new TimePicker({
                        name: "time",
                        value: this.value,
                        intermediateChanges: true,
                        disabled: this.disabled
                    }, timeNode);
                }
                else
                {
                    this.timeEditor = new dijit.form.TimeTextBox({
                        name: "time",
                        intermediateChanges: true,
                        value: this.value,
                        constraints: {
                            timePattern: 'HH:mm:ss.SSS',
                            clickableIncrement: 'T00:15:00',
                            visibleIncrement: 'T00:15:00',
                            visibleRange: 'T00:00:00'
                        }
                    }, timeNode);
                }
                this.timeEditor.on("change", lang.hitch(this, this._setValue));
            },
            startup: function ()
            {
                this.inherited(arguments);
                this.dateEditor.startup();
                this.timeEditor.startup();
            },
            _setValue: function ()
            {
                var date = this.dateEditor.get("value");
                if (date)
                {
                    var time = this.timeEditor.value;
                    var value = date.getTime() + (time ? time.getTime() + time.getTimezoneOffset() * 60 * 1000 : 0);
                    this.value = value;
                    this._buildExpression(value);
                    this.emit("change", this.value);
                }
            },
            _buildExpression: function (value)
            {
                var formattedDate = this.userPreferences.formatDateTime(value, {
                    selector: "date",
                    datePattern: "yyyy-MM-dd"
                });
                var formattedTime = this.userPreferences.formatDateTime(value, {
                    selector: "time",
                    datePattern: "HH:mm:ss.SSS"
                });
                var timeZoneOffset = "";
                var timeZone = this.userPreferences.getTimeZoneInfo();
                if (timeZone)
                {
                    var timeZoneOffsetInMinutes = timeZone.offset;
                    if (!timeZoneOffsetInMinutes)
                    {
                        timeZoneOffset = "Z";
                    }
                    else
                    {
                        if (timeZoneOffsetInMinutes > 0)
                        {
                            timeZoneOffset = "+";
                        }
                        timeZoneOffset =
                            timeZoneOffset + number.format(timeZoneOffsetInMinutes / 60, {pattern: "00"}) + ":"
                            + number.format(timeZoneOffsetInMinutes % 60, {pattern: "00"});
                    }
                }
                this.expression = "to_date('" + formattedDate + "T" + formattedTime + timeZoneOffset + "')";
            },
            _getExpressionAttr: function ()
            {
                return this.expression;
            },
            _setDisabledAttr: function (value)
            {
                this.inherited(arguments);
                this.disabled = value;
                this.dateEditor.set("disabled", value);
                this.timeEditor.set("disabled", value);
            },
            _setValueAttr: function (value)
            {
                var date;
                if (value instanceof Date)
                {
                    date = value;
                }
                else if (isInt(value))
                {
                    date = new Date(value);
                }
                if (date)
                {
                    this.dateEditor.set("value", date);
                    this.timeEditor.set("value", date);
                    this.value = date;
                }
                this.inherited(arguments);
            },
            _getValueAttr: function ()
            {
                return this.value;
            },
            isValid: function ()
            {
                return !!this.value;
            }
        });

        return declare("qpid.management.query.CriteriaPane",
            [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
            {
                //Strip out the apache comment header from the template html as comments unsupported.
                templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

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
                userPreferences: null,

                /**
                 * auxiliary fields
                 */
                _removed: false,
                _stored: false,
                _lastCondition: undefined,
                _savedValue: undefined,
                _savedCondition: undefined,

                postCreate: function ()
                {
                    this.inherited(arguments);
                    this._postCreate();
                },
                _postCreate: function ()
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
                        for (var i = 0; i < this.typeValidValues.length; i++)
                        {
                            var val = this.typeValidValues[i];
                            domConstruct.create("option", {
                                innerHTML: entities.encode(String(val)),
                                value: val
                            }, domNode);
                        }
                        this.valueEditor = new dijit.form.MultiSelect({
                            intermediateChanges: true,
                            multiple: true
                        }, domNode);
                    }
                    else if (this.typeName === "Date")
                    {
                        this.valueEditor = new DateTimePicker({
                            intermediateChanges: true,
                            userPreferences: this.userPreferences
                        }, domNode);
                    }
                    else
                    {
                        this.valueEditor = isNumericType(this.typeName) ? new dijit.form.NumberTextBox({
                            value: 0,
                            required: true,
                            intermediateChanges: true,
                            invalidMessage: 'Please enter a numeric value.'
                        }, domNode) : new dijit.form.ValidationTextBox({
                            value: '',
                            required: true,
                            intermediateChanges: true
                        }, domNode);
                    }
                    this.valueEditor.startup();
                    this.valueEditor.on("change", lang.hitch(this, this._conditionValueChanged));
                    this.removeCriteria.on("click", lang.hitch(this, this._removalRequested));
                    this._conditionChanged(ANY);
                    this._saveState()
                },
                _saveState: function ()
                {
                    this._savedValue = this.valueEditor.value;
                    this._savedCondition = this.criteriaCondition.value;
                },
                _conditionChanged: function (newValue)
                {
                    if (this.criteriaCondition.value != this._lastCondition)
                    {
                        var editable = conditionHasWidget(newValue);
                        this.valueEditor.set("disabled", !editable);
                        this._lastCondition = newValue;
                        this._conditionValueChanged();
                    }
                },
                _conditionValueChanged: function (newValue)
                {
                    var expression;
                    var val = this._getConditionValue();
                    if (val)
                    {
                        expression =
                            buildExpression(this.criteriaName, this.criteriaCondition.value, val, this.typeName);
                    }
                    if (!expression)
                    {
                        expression = this.criteriaName + ":" + ANY;
                    }
                    this.conditionExpression.innerHTML = entities.encode(String(expression));

                    // notify listeners that criteria is changed
                    this.emit("change", this);
                },
                getExpression: function ()
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
                getCondition: function () {
                    if (this._removed)
                    {
                        return undefined;
                    }
                    return {
                        name: this.criteriaName,
                        value: this._getConditionValue(),
                        operator: this.criteriaCondition.value,
                        type: this.typeName
                    };
                },
                _getConditionValue: function ()
                {
                    return this.valueEditor.expression || this.valueEditor.value;
                },
                _removalRequested: function ()
                {
                    this._removed = true;
                    this.emit("change", this);
                },
                _getRemovedAttr: function ()
                {
                    return this._removed;
                },
                cancelled: function ()
                {
                    if (!this._stored)
                    {
                        this.destroyRecursive(false);
                    }
                    else
                    {
                        if (this._removed)
                        {
                            domStyle.set(this.domNode, "display", "");
                            this._removed = false;
                        }
                        this.valueEditor.set("value", this._savedValue);
                        this.criteriaCondition.set("value", this._savedCondition);
                    }
                },
                submitted: function ()
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
                _setRemovableAttr: function (removable)
                {
                    this._removable = removable
                    this.removeCriteria.set("disabled", !removable);
                },
                isValidCriteria: function ()
                {
                    if (!this.valueEditor.get("disabled"))
                    {
                        if (this.valueEditor.isValid)
                        {
                            return this.valueEditor.isValid();
                        }

                        if (this.valueEditor instanceof dijit.form.MultiSelect)
                        {
                            var value = this.valueEditor.value;
                            return value && value.length;
                        }
                    }
                    return true;
                }

            });

    });