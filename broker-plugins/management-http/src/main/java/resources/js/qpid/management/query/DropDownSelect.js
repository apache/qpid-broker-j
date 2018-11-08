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
        "dojo/_base/lang",
        "dojo/json",
        "dojo/dom-construct",
        "qpid/management/query/OptionsPanel",
        "dijit/popup",
        "dojo/Evented",
        "dijit/TooltipDialog",
        "dijit/_WidgetBase",
        "dojo/domReady!"],
    function (declare,
              lang,
              json,
              domConstruct,
              OptionsPanel,
              popup,
              Evented)
    {

        return declare("qpid.management.query.DropDownSelect", [dijit._WidgetBase, Evented], {
            _selectButton: null,
            _optionsDialog: null,
            _optionsPanel: null,
            _selectedItems: null,

            postCreate: function ()
            {
                this.inherited(arguments);
                this._postCreate();
            },
            _postCreate: function ()
            {
                this._optionsPanel = new OptionsPanel({
                    showButtons: true,
                    showSummary: true,
                    renderItem: this.renderItem
                }, this._createDomNode());
                this._optionsDialog = new dijit.TooltipDialog({content: this._optionsPanel}, this._createDomNode());
                this._selectButton = new dijit.form.DropDownButton({
                    label: this.label || "Select",
                    dropDown: this._optionsDialog,
                    iconClass: this.iconClass
                }, this._createDomNode());
                this._optionsPanel.doneButton.on("click", lang.hitch(this, this._onSelectionDone));
                this._optionsPanel.cancelButton.on("click", lang.hitch(this, this._hideAndResetSearch));
                this._optionsDialog.on("hide", lang.hitch(this, this._resetSearch));
                this._optionsDialog.on("show", lang.hitch(this, this._onShow));
                this._selectButton.startup();
                this._optionsPanel.startup();
                this._optionsDialog.startup();
            },
            _createDomNode: function ()
            {
                return domConstruct.create("span", null, this.domNode);
            },
            _setDataAttr: function (data)
            {
                this._optionsPanel.set("data", data);
                this._selectedItems = this._optionsPanel.get("selectedItems");
            },
            _getSelectedItemsAttr: function ()
            {
                return this._optionsPanel.get("selectedItems");
            },
            _onSelectionDone: function ()
            {
                this._selectedItems = this._optionsPanel.get("selectedItems");
                popup.close(this._optionsDialog);
                this._optionsPanel.resetItems();
                this.emit("change", lang.clone(this._selectedItems));
            },
            _hideAndResetSearch: function ()
            {
                popup.close(this._optionsDialog);
                this._resetSearch();
            },
            _resetSearch: function ()
            {
                this._optionsPanel.resetItems(this._selectedItems);
            },
            _setDisabledAttr: function (value)
            {
                this._selectButton.set("disabled", value);
            },
            _getDisabledAttr: function ()
            {
                return this._selectButton.get("disabled")
            },
            _onShow: function ()
            {
                this._optionsPanel.resizeGrid();
            },
            _setRenderItemAttr: function(renderItem)
            {
                if (this._optionsPanel)
                {
                    this._optionsPanel.set("renderItem", renderItem);
                }
                else
                {
                    this.renderItem = renderItem;
                }
            }
        });
    });
