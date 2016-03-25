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
  "dojo/text!query/WhereCriteria.html",
  "dojox/html/entities",
  "dijit/popup",
  "qpid/management/query/CriteriaPane",
  "qpid/common/FormWidgetMixin",
  "dijit/_Widget",
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
  "dojo/domReady!"
],
function(declare, array, lang, string, template, entities, popup, CriteriaPane)
{
    return declare("qpid.management.query.WhereCriteria",
                   [dijit._Widget, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, qpid.common.FormWidgetMixin],
                   {
                        //Strip out the apache comment header from the template html as comments unsupported.
                        templateString:    template.replace(/<!--[\s\S]*?-->/g, ""),
                        constructor: function(args)
                                     {
                                        this._attributeDetails = args.attributeDetails;
                                        this.onChange = args.onChange;
                                        this.inherited(arguments);
                                     },
                        postCreate:  function()
                                     {
                                        this.inherited(arguments);
                                        this._postCreate();
                                     },
                        _postCreate: function()
                                     {
                                       this.removeCriteria.on("click", lang.hitch(this, this._destroy));
                                       this.doneButton.on("click", lang.hitch(this, this._criteriaSet));
                                       this.cancelButton.on("click", lang.hitch(this, this._dialogCancelled));
                                       this.addButton.on("click", lang.hitch(this, this._addCriteria));
                                       this.expressionMatchCondition.on("change", lang.hitch(this, this._criteriaConditionChanged));
                                       var criteriaPane = this._addCriteria({_stored:true});
                                       criteriaPane.submitted();
                                       this._displayExpression();
                                       this._criteriaConditionChanged();
                                       this.editDialog.on("hide", lang.hitch(this, this._dialogHidden));
                                     },
                        _addCriteria:function()
                                     {
                                       var criteriaPane = new CriteriaPane({criteriaName: this._attributeDetails.attributeName,
                                                                            typeName: this._attributeDetails.type,
                                                                            typeValidValues: this._attributeDetails.validValues});
                                       this.criteriaContainer.addChild(criteriaPane);
                                       criteriaPane.on("change", lang.hitch(this, this._criteriaConditionChanged));
                                       this._updateRemovable();
                                       this.conditionDialogContent.connectChildren();
                                       return criteriaPane;
                                     },
                        _getNumberOfCriteria : function()
                                     {
                                           var counter = 0;
                                           var criteriaArray = this.criteriaContainer.getChildren();
                                           for(var i = 0;i<criteriaArray.length;i++)
                                           {
                                              if (!criteriaArray[i]._removed)
                                              {
                                                counter = counter + 1;
                                              }
                                           }
                                           return counter;
                                     },
                        _updateRemovable: function()
                                     {
                                          var counter = this._getNumberOfCriteria();
                                          var singleCriteria = counter == 1;
                                          var criteriaArray = this.criteriaContainer.getChildren();
                                          for(var i = 0;i<criteriaArray.length;i++)
                                          {
                                             if (!criteriaArray[i]._removed)
                                             {
                                                criteriaArray[i].setRemovable(!singleCriteria);
                                             }
                                          }
                                          this.expressionMatchCondition.set("disabled", singleCriteria);
                                     },
                        _getUserFriendlyExpression: function()
                                      {
                                        var expression = this.getConditionExpression();
                                        if (!expression)
                                        {
                                          expression = this._attributeDetails.attributeName + ": any";
                                        }
                                        return expression;
                                      },
                        _displayExpression: function()
                                     {
                                       var expression = this._getUserFriendlyExpression();
                                       this.criteria.set("label", expression);
                                     },
                        _criteriaConditionChanged: function()
                                    {
                                      this.conditionDialogContent.connectChildren();
                                      var isValid =  this._validateCriteria();
                                      if (isValid)
                                      {
                                        var expression = this._getUserFriendlyExpression();
                                        this.newColumnCondition.set("value", expression);
                                        this._updateRemovable();
                                      }
                                      this.doneButton.set("disabled", !isValid);
                                    },
                        _validateCriteria:function()
                                    {
                                      var isValid = true;
                                      var criteriaArray = this.criteriaContainer.getChildren();
                                      for(var i = 0;i<criteriaArray.length;i++)
                                      {
                                        if (!criteriaArray[i].validate())
                                        {
                                           isValid = false;
                                        }
                                      }
                                      return isValid;
                                    },
                        getAttributeDetails: function()
                                    {
                                      return this._attributeDetails;
                                    },
                        getConditionExpression:function()
                                    {
                                      var expression = "";
                                      var criteriaArray = this.criteriaContainer.getChildren();
                                      var criteriaCounter = 0;
                                      for(var i = 0;i<criteriaArray.length;i++)
                                      {
                                          var criteria = criteriaArray[i].getExpression();
                                          if (criteria)
                                          {
                                              if (expression)
                                              {
                                                 expression = expression + " " + this.expressionMatchCondition.value;
                                              }
                                              expression = expression + " " + criteria;
                                              criteriaCounter = criteriaCounter + 1;
                                          }
                                      }
                                      if (criteriaCounter>0 && this.expressionMatchCondition.value == "or")
                                      {
                                        expression = "( " + expression + " )"
                                      }
                                      return expression;
                                    },
                        _destroy:   function()
                                    {
                                        var onChange = this.onChange;
                                        var parentContainer = dijit.getEnclosingWidget(this);
                                        if (parentContainer!= null && parentContainer instanceof  dijit._Container)
                                        {
                                            parentContainer.removeChild(this);

                                        }

                                        // notify listeners which are listening for onChange events
                                        this.set("value", undefined);

                                        this.destroyRecursive(false);

                                        if (onChange)
                                        {
                                            onChange();
                                        }
                                    },
                        _criteriaSet: function()
                                    {
                                      var isValid = this._validateCriteria();
                                      if (isValid)
                                      {
                                          this._displayExpression();
                                          var criteriaArray = this.criteriaContainer.getChildren();
                                          for(var i = 0;i<criteriaArray.length;i++)
                                          {
                                              criteriaArray[i].submitted();
                                          }
                                          popup.close(this.editDialog);

                                          // notify listeners which are listening for onChange events
                                          this.set("value", this.getConditionExpression());
                                          var onChange = this.onChange;
                                          if (onChange)
                                          {
                                                onChange();
                                          }
                                      }
                                    },
                        _dialogCancelled: function()
                                    {
                                      popup.close(this.editDialog);
                                      this._dialogHidden();
                                    },
                        _dialogHidden: function()
                                    {
                                        var criteriaArray = this.criteriaContainer.getChildren();
                                        for(var i = 0;i<criteriaArray.length;i++)
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