define([
    'dojo/_base/lang',
    'dojo/_base/declare',
    'dstore/Store',
    'dstore/QueryResults'
], function (lang, declare, Store, QueryResults) {

    return declare("qpid.management.queue.MessageStore", [Store], {

        modelObject: null,
        management: null,
        operationName: "getMessageInfo",
        operationArguments: {includeHeaders: false},
        totalLength: null,

        fetch: function () {
            return this._request();
        },

        fetchRange: function (kwArgs) {
            return this._request(kwArgs);
        },

        _request: function (kwArgs) {
            var modelObj = {
                name: this.operationName,
                parent: this.modelObject,
                type: this.modelObject.type
            };
            var query = lang.clone(this.operationArguments);
            if (kwArgs && kwArgs.hasOwnProperty("start"))
            {
                query.first = kwArgs.start;
            }
            if (kwArgs && kwArgs.hasOwnProperty("end"))
            {
                query.last = kwArgs.end;
            }
            var headers = lang.mixin({Accept: "application/javascript, application/json"}, kwArgs.headers);
            var messages = this.management.invoke(modelObj, query, {headers: headers}, true);
            var depth = this._getQueueDepth();
            return new QueryResults(messages, {totalLength: depth});
        },

        _getQueueDepth: function () {
            var modelObj = {
                name: "getStatistics",
                parent: this.modelObject,
                type: this.modelObject.type
            };
            return this.management.load(modelObj, {statistics: ["queueDepthMessages"]})
                .then(function (data) {
                    return data["queueDepthMessages"];
                }, function (error) {
                    return 0;
                });
        }

    });
});
