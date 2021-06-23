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
define(["dojo/parser",
        "dojo/dom-construct",
        "dijit/registry",
        "dojo/_base/event",
        "qpid/common/util",
        "dojo/text!connectionlimitprovider/rulebased/rule.html",
        "dijit/Dialog",
        "dijit/form/Button",
        "dijit/form/Form",
        "dijit/form/TextBox",
        "dijit/form/ValidationTextBox",
        "dijit/form/NumberTextBox",
        "dijit/form/CheckBox",
        "dojo/domReady!"],
    function (parser,
              construct,
              registry,
              event,
              util,
              template)
    {
        function Rule()
        {
            const that = this;
            this.containerNode = construct.create("div", {innerHTML: template});

            parser.parse(this.containerNode)
                .then(function ()
                {
                    that._dialog = registry.byId("connectionLimitRule");
                    that._form = registry.byId("connectionLimitRule.form");
                    that._formBox = util.findNode("formBox", that.containerNode)[0];

                    that._saveButton = registry.byId("connectionLimitRule.saveButton");
                    that._saveButton.on("click", function (e)
                    {
                        that._saveRule(e);
                    });
                    that._deleteButton = registry.byId("connectionLimitRule.deleteButton");
                    that._deleteButton.on("click", function (e)
                    {
                        that._deleteRule(e);
                    });
                    that._cancelButton = registry.byId("connectionLimitRule.cancelButton");
                    that._cancelButton.on("click", function (e)
                    {
                        that._cancel(e);
                    });
                });
        }

        Rule.prototype.show = function (data)
        {
            this.management = data.management;
            this.modelObj = data.modelObj;

            this._gridData = [];
            this._isNew = true;
            let current = {id: undefined};

            for (const rule of data.gridData)
            {
                if (data.id === rule.id)
                {
                    this._gridData.push(Object.assign(current, rule));
                    this._isNew = false;
                }
                else
                {
                    this._gridData.push(rule);
                }
            }
            this._currentRule = current;

            this._form.reset();
            util.applyToWidgets(this._formBox,
                data.category,
                data.type,
                current,
                data.management.metadata,
                {});

            this._dialog.show();
        };

        Rule.prototype._cancel = function (e)
        {
            event.stop(e);
            this._dialog.hide();
        };

        Rule.prototype._saveRule = function (e)
        {
            event.stop(e);
            if (this._form.validate())
            {
                this._deleteButton.set("disabled", true);
                this._saveButton.set("disabled", true);

                delete this._currentRule.port;
                delete this._currentRule.blocked;
                delete this._currentRule.countLimit;
                delete this._currentRule.frequencyLimit;
                delete this._currentRule.frequencyPeriod;

                Object.assign(this._currentRule, util.getFormWidgetValues(this._form, {}));
                if (this._isNew)
                {
                    this._gridData.push(this._currentRule);
                    this._isNew = false;
                }
                this._submit();
            }
            else
            {
                alert('Form contains invalid data. Please correct first');
            }
        };

        Rule.prototype._deleteRule = function (e)
        {
            event.stop(e);
            if (!this._isNew)
            {
                this._deleteButton.set("disabled", true);
                this._saveButton.set("disabled", true);
                let i = 0;
                while (i < this._gridData.length)
                {
                    if (this._gridData[i].id === this._currentRule.id)
                    {
                        this._gridData.splice(i, 1);
                    }
                    else
                    {
                        i++;
                    }
                }
                this._isNew = true;
                this._submit();
            }
        };

        Rule.prototype._submit = function ()
        {
            const data = [];
            for (const rule of this._gridData)
            {
                const r = Object.assign({}, rule);
                delete r.id;
                data.push(r);
            }

            const that = this;
            this.management.update(this.modelObj, {rules: data})
                .then(that._dialog.hide.bind(that._dialog), this.management.xhrErrorHandler)
                .always(function ()
                {
                    that._deleteButton.set("disabled", false);
                    that._saveButton.set("disabled", false);
                });
        }

        return new Rule();
    });
