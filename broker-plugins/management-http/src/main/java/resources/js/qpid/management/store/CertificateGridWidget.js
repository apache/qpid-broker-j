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
        "dojo/Evented",
        "dojo/parser",
        "dojo/query",
        "dojo/dom-class",
        "dijit/registry",
        "qpid/common/util",
        "qpid/common/UpdatableStore",
        "dojox/grid/EnhancedGrid",
        "dojo/text!store/CertificateGridWidget.html",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dojo/domReady!"],
    function (declare,
              lang,
              Evented,
              parser,
              query,
              domClass,
              registry,
              util,
              UpdatableStore,
              EnhancedGrid,
              template)
{
    return declare("qpid.management.store.CertificateGridWidget",
        [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
        {
            //Strip out the apache comment header from the template html as comments unsupported.
            templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

            // template fields
            certificatesGridContainer: null,
            removeCertificateButton: null,
            certificateUploader: null,
            certificateControls: null,

            // constructor parameters
            management: null,

            // other
            certificatesGrid: null,

            postCreate : function ()
            {
                this.inherited(arguments);

                var gridProperties = {
                    height: 400,
                    selectionMode: "extended",
                    plugins: {
                        indirectSelection: true,
                        pagination: {
                            pageSizes: [10, 25, 50, 100],
                            description: true,
                            sizeSwitch: true,
                            pageStepper: true,
                            gotoButton: true,
                            maxPageStep: 4,
                            position: "bottom"
                        }
                    }
                };

                this.certificatesGrid =
                    new UpdatableStore([], this.certificatesGridContainer, [{
                        name: "Subject Name",
                        field: "subjectName",
                        width: "25%"
                    }, {
                        name: "Issuer Name",
                        field: "issuerName",
                        width: "25%"
                    }, {
                        name: "Serial #",
                        field: "serialNumber",
                        width: "10%"
                    }, {
                        name: "Valid From",
                        field: "validFrom",
                        width: "20%",
                        formatter: lang.hitch(this, this._formatDate)
                    }, {
                        name: "Valid Until",
                        field: "validUntil",
                        width: "20%",
                        formatter: lang.hitch(this, this._formatDate)
                    }], null, gridProperties, EnhancedGrid);

                if (window.FileReader)
                {
                    this.certificateUploader.on("change", lang.hitch(this,this._onFileSelected));
                }
                else
                {
                    this.certificateUploader.set("disabled", true);
                    this.certificateUploader.domNode.style.display = "none";
                }
                this.removeCertificateButton.on("click", lang.hitch(this, this._removeCertificates));
            },
            enableCertificateControls: function(enabled)
            {
                this.certificatesGrid.grid.layout.setColumnVisibility(0, enabled);
                if (enabled)
                {
                    domClass.remove(this.certificateControls, "dijitHidden");
                }
                else
                {
                    domClass.add(this.certificateControls, "dijitHidden");
                }
            },
            destroy: function ()
            {
                this.inherited(arguments);
                this.certificatesGrid.close();
            },
            resize: function ()
            {
                this.inherited(arguments);
                this.certificatesGrid.grid.resize();
            },
            update : function (certificates)
            {
                var certItems = this._addIdToCertificates(certificates);
                this.certificatesGrid.grid.beginUpdate();
                this.certificatesGrid.update(certItems);
                this.certificatesGrid.grid.endUpdate();
            },
            _addIdToCertificates : function(certDetails)
            {
                var certItems = [];
                for (var idx in certDetails)
                {
                    var item = lang.mixin(certDetails[idx],
                        {id: certDetails[idx].serialNumber + '|' + certDetails[idx].issuerName});
                    certItems.push(item);
                }
                return certItems;
            },
            _onFileSelected: function ()
            {
                if (this.certificateUploader.domNode.children[0].files)
                {
                    this.certificateUploader.set("disabled", true);
                    var file = this.certificateUploader.domNode.children[0].files[0];
                    var fileReader = new FileReader();
                    fileReader.onload = lang.hitch(this, function (evt)
                    {
                        var result = fileReader.result;
                        if (result.indexOf("-----BEGIN CERTIFICATE-----") != -1)
                        {
                            this._uploadCertificate(result);
                        }
                        else
                        {
                            fileReader.onload = lang.hitch(this, function (evt)
                            {
                                var binresult = fileReader.result;
                                binresult = binresult.substring(binresult.indexOf(",") + 1);
                                this._uploadCertificate(binresult);
                            });
                            fileReader.readAsDataURL(file);
                        }
                    });
                    fileReader.readAsText(file);
                }
            },
            _uploadComplete: function ()
            {
                this.certificateUploader.set("disabled", false);
                this.certificateUploader.reset();
            },
            _uploadError: function (error)
            {
                this.management.errorHandler(error);
                this.certificateUploader.set("disabled", false);
                this.certificateUploader.reset();
            },
            _uploadCertificate : function (cert)
            {
                var parentModelObj = this.modelObj;
                var modelObj = {
                    type: parentModelObj.type,
                    name: "addCertificate",
                    parent: parentModelObj
                };
                var url = this.management.buildObjectURL(modelObj);

                this.management.post({url: url}, {certificate: cert})
                    .then(lang.hitch(this, function ()
                        {
                            this._uploadComplete();
                            this.emit("certificatesUpdate");
                        }),
                        lang.hitch(this, this._uploadError));
            },
            _removeCertificates : function ()
            {
                var data = this.certificatesGrid.grid.selection.getSelected();

                if (data.length)
                {
                    this.removeCertificateButton.set("disabled", true);
                    var parentModelObj = this.modelObj;
                    var modelObj = {
                        type: parentModelObj.type,
                        name: "removeCertificates",
                        parent: parentModelObj
                    };
                    var items = [];
                    for (var i = 0; i < data.length; i++)
                    {
                        var parts = data[i].id.split("|");
                        items.push({
                            issuerName: parts[1],
                            serialNumber: parts[0]
                        });
                    }
                    var url = this.management.buildObjectURL(modelObj);
                    this.management.post({url: url}, {certificates: items})
                        .then(null, management.xhrErrorHandler)
                        .always(lang.hitch(this, function ()
                        {
                            this.removeCertificateButton.set("disabled", false);
                            this.emit("certificatesUpdated");
                        }));
                }
            },
            _formatDate: function (val)
            {
                return this.management.userPreferences.formatDateTime(val, {
                    addOffset: true,
                    appendTimeZone: true
                });
            }
        }
    );
});
