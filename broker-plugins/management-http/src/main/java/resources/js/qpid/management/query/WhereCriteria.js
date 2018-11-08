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
        "dojo/text!query/WhereCriteria.html",
        "dojox/html/entities",
        "dijit/popup",
        "qpid/management/query/CriteriaPane",
        "dojo/Evented",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dijit/layout/ContentPane",
        "dijit/form/Button",
        "dijit/form/ValidationTextBox",
        "dijit/form/TextBox",
        "dijit/form/Select",
        "dijit/form/Form",
        "dijit/_Container",
        "dijit/form/SimpleTextarea",
        "dijit/InlineEditBox",
        "dojo/domReady!"], function (declare, array, lang, string, template, entities, popup, CriteriaPane, Evented)
{
    return declare("qpid.management.query.WhereCriteria",
        [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
        {
            //Strip out the apache comment header from the template html as comments unsupported.
            templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

            /**
             * template attach points
             */
            removeCriteria: null,
            doneButton: null,
            cancelButton: null,
            addButton: null,
            criteriaMatchCondition: null,
            editDialog: null,
            conditionDialogContent: null,
            criteriaContainer: null,
            newColumnCondition: null,

            /**
             * constructor arguments
             */
            attributeDetails: null,
            userPreferences: null,

            /**
             * inner fields
             */
            _deleted: false,

            constructor: function (args)
            {
                this.attributeDetails = args.attributeDetails;
                this.inherited(arguments);
            },
            postCreate: function ()
            {
                this.inherited(arguments);
                this._postCreate();
            },
            _getDeletedAttr: function ()
            {
                return this._deleted;
            },
            _postCreate: function ()
            {
                this.removeCriteria.on("click", lang.hitch(this, this._destroy));
                this.doneButton.on("click", lang.hitch(this, this._criteriaSet));
                this.cancelButton.on("click", lang.hitch(this, this._dialogCancelled));
                this.addButton.on("click", lang.hitch(this, this._addCriteria));
                this.criteriaMatchCondition.on("change", lang.hitch(this, this._criteriaConditionChanged));
                var criteriaPane = this._addCriteria({_stored: true});
                criteriaPane.submitted();
                this._displayExpression();
                this._criteriaConditionChanged();
                this.editDialog.on("hide", lang.hitch(this, this._dialogHidden));
            },
            _addCriteria: function ()
            {
                var criteriaPane = new CriteriaPane({
                    criteriaName: this.attributeDetails.attributeName,
                    typeName: this.attributeDetails.type,
                    typeValidValues: this.attributeDetails.validValues,
                    userPreferences: this.userPreferences
                });
                this.criteriaContainer.addChild(criteriaPane);
                criteriaPane.on("change", lang.hitch(this, this._criteriaChanged));
                this._updateRemovable();
                return criteriaPane;
            },
            _getNumberOfCriteria: function ()
            {
                var counter = 0;
                var criteriaArray = this.criteriaContainer.getChildren();
                for (var i = 0; i < criteriaArray.length; i++)
                {
                    if (!criteriaArray[i].get("removed"))
                    {
                        counter = counter + 1;
                    }
                }
                return counter;
            },
            _updateRemovable: function ()
            {
                var counter = this._getNumberOfCriteria();
                var singleCriteria = counter == 1;
                var criteriaArray = this.criteriaContainer.getChildren();
                for (var i = 0; i < criteriaArray.length; i++)
                {
                    if (!criteriaArray[i].get("removed"))
                    {
                        criteriaArray[i].set("removable", !singleCriteria);
                        break;
                    }
                }
                this.criteriaMatchCondition.set("disabled", singleCriteria);
            },
            _getUserFriendlyExpression: function ()
            {
                var expression = this.getConditionExpression();
                if (!expression)
                {
                    expression = this.attributeDetails.attributeName + ": any";
                }
                return expression;
            },
            _displayExpression: function ()
            {
                var expression = this._getUserFriendlyExpression();
                this.criteria.set("label", expression);
            },
            _criteriaConditionChanged: function ()
            {
                var isValid = this._validateCriteria();
                if (isValid)
                {
                    var expression = this._getUserFriendlyExpression();
                    this.newColumnCondition.set("value", expression);
                    this._updateRemovable();
                }
                this.doneButton.set("disabled", !isValid);
            },
            _criteriaChanged: function (criteria)
            {
                this._criteriaConditionChanged();
                if (criteria && criteria.get("removed"))
                {
                    var prev = this.criteriaMatchCondition;
                    var criteriaArray = this.criteriaContainer.getChildren();
                    for (var i = 0; i < criteriaArray.length; i++)
                    {
                        if (criteriaArray[i] == criteria)
                        {
                            break;
                        }
                        if (!criteriaArray[i].get("removed"))
                        {
                            prev = criteriaArray[i]
                        }
                    }

                    if (prev)
                    {
                        if (prev.focus)
                        {
                            prev.focus();
                        }
                        else if (prev instanceof qpid.management.query.CriteriaPane)
                        {
                            prev.criteriaCondition.focus();
                        }
                        criteria.domNode.style.display = "none";
                    }
                }
            },
            _validateCriteria: function ()
            {
                var isValid = true;
                var criteriaArray = this.criteriaContainer.getChildren();
                for (var i = 0; i < criteriaArray.length; i++)
                {
                    if (!criteriaArray[i].get("removed") && !criteriaArray[i].isValidCriteria())
                    {
                        isValid = false;
                    }
                }
                return isValid;
            },
            _getAttributeDetailsAttr: function ()
            {
                return this.attributeDetails;
            },
            getConditionExpression: function ()
            {
                if (this._deleted)
                {
                    return undefined;
                }

                var expression = "";
                var criteriaArray = this.criteriaContainer.getChildren();
                var criteriaCounter = 0;
                for (var i = 0; i < criteriaArray.length; i++)
                {
                    var criteria = criteriaArray[i].getExpression();
                    if (criteria)
                    {
                        if (expression)
                        {
                            expression = expression + " " + this.criteriaMatchCondition.value;
                        }
                        expression = expression + " " + criteria;
                        criteriaCounter = criteriaCounter + 1;
                    }
                }
                if (criteriaCounter > 0 && this.criteriaMatchCondition.value == "or")
                {
                    expression = "( " + expression + " )"
                }
                return expression;
            },
            getConditions: function () {
                if (this._deleted)
                {
                    return undefined;
                }

                var conditions = [];
                var criteriaArray = this.criteriaContainer.getChildren();
                for (var i = 0; i < criteriaArray.length; i++)
                {
                    var condition = criteriaArray[i].getCondition();
                    if (condition)
                    {
                        conditions.push(condition);
                    }
                }

                return {
                    operator: this.criteriaMatchCondition.value,
                    conditions: conditions
                };
            },
            _destroy: function ()
            {
                this._deleted = true;
                try
                {
                    // notify listeners which are listening for onChange events
                    this.emit("change", this);
                }
                finally
                {
                    this.destroyRecursive(false);
                }
            },
            _criteriaSet: function ()
            {
                var isValid = this._validateCriteria();
                if (isValid)
                {
                    this._displayExpression();
                    var criteriaArray = this.criteriaContainer.getChildren();
                    for (var i = 0; i < criteriaArray.length; i++)
                    {
                        criteriaArray[i].submitted();
                    }
                    popup.close(this.editDialog);

                    // notify listeners which are listening for onChange events
                    this.emit("change", this);
                }
            },
            _dialogCancelled: function ()
            {
                popup.close(this.editDialog);
                this._dialogHidden();
            },
            _dialogHidden: function ()
            {
                var criteriaArray = this.criteriaContainer.getChildren();
                for (var i = 0; i < criteriaArray.length; i++)
                {
                    if (criteriaArray[i].cancelled)
                    {
                        criteriaArray[i].cancelled();
                    }
                }
                this._updateRemovable();
            }
        });
});