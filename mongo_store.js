var _ = require('underscore');
var store = require('./store')
//var dummy = require('./dummy')
var valuesLib = require('../../utils/values')
var mongodb = require('mongodb')
var mongoTab = require('./mongo_tab')
var cl = require('../../utils/console_log').server_log




var MongoStore = exports.Store = function (options) {
  options || (options = {})
//  options = _.defaults(options, config.db)
  store.Store.call(this, options)
  _.extend(this, _.pick(options, 'host', 'port', 'connection_pool'))
  this.define_write_concern(options)
  this.tabs = {}
}

valuesLib.inherit(MongoStore, store.Store)

MongoStore.prototype.type = 'MongoStore'

MongoStore.prototype.caption = 'БД mongodb'

  
MongoStore.prototype.on_open = function () {
  this.sequenceCollection = this.db.collection('sys_sequences')
//  cl('in on_open')
}

MongoStore.prototype.on_close = function (callback) {
  delete this.sequenceCollection
}


// callback в виде function(err, MongoStore)
MongoStore.prototype.open_func = function (callback) {
  mongodb.MongoClient.connect(
    "mongodb://" + this.host + ":" + this.port + "/" + this.name, 
    {server: {poolSize: this.connection_pool}}, 
    callback
  )
}

//MongoStore.prototype.open = function (callback) {
//  this._open(
//    function (cb) {
//      mongodb.MongoClient.connect(
//        "mongodb://" + this.host + ":" + this.port + "/" + this.name, 
//        {server: {poolSize: this.connection_pool}}, 
//        cb
//      )
//    },
//    callback
//  )
//}


MongoStore.prototype.close_func = function(callback) {
  if (this.db) this.db.close(true, callback)
  else if (callback) callback()
}

MongoStore.prototype.define_write_concern = function (options) {
  var ws = this.get_param(options, 'write_safety')
  this.write_safety = ws
  this.write_concern = {}
  if (!ws) this.write_concern.j = true
  else if (ws === 'none') this.write_concern.w = 0
  else if (ws === 'no_guarantee') this.write_concern.w = 1
  else {
    this.write_concern.w = 1
    this.write_concern.wtimeout = 5000 
    if (valuesLib.in_comma_str(ws, 'journal')) 
      this.write_concern.j = true
    if (valuesLib.in_comma_str(ws, 'one_replica')) 
      this.write_concern.w = 2
    if (valuesLib.in_comma_str(ws, 'majority_replica')) 
      this.write_concern.w = 'majority'
  }
}

/**
@function
@param {Object|string} options если параметр - строка и если таблица уже была открыта,
возвращает уже октрытую, если параметр - объект в параметре опции для открытия таблицы заново
*/
MongoStore.prototype.tab = function(options) {
  if (!this.db) 
    this.logError('tab(), попытка в неоткрытой БД открыть таблицу: ' + options)
  else if (_.isString(options) || (valuesLib.is_object(options) && options.name)) {
    if (_.isString(options)) {
      if (this.tabs[options]) return this.tabs[options]
      else options = {name: options}
    }
    else if (this.tabs[options.name]) return this.tabs[options.name]
    var tab
    options.store = this
    tab = new mongoTab.Tab(options)
    if (tab.name) this.tabs[tab.name] = tab
    return tab
  }
  else this.logError('tab(), неверный параметр: ' + options)
}


MongoStore.prototype.dismissTab = function(name) {
  if (this.tabs[name]) delete this.tabs[name]
}
  

//MongoStore.prototype.tab = function(options) {
//  if (!this.db) 
//    this.logError('tab(), попытка в неоткрытой БД открыть таблицу: ' + options)
////  else if (_.isString(options) && this.tabs[options]) ()
////    return this.tabs[options]
//  else if (_.isString(options) || valuesLib.is_object(options)) {
//    if (_.isString(options)) options = {name: options}
//    if (this.tabs[options.name]) return this.tabs[options.name]
//    else {
//      var tab
//      options.store = this
//      tab = new MongoTab(options)
//      if (tab.name) this.tabs[tab.name] = tab
//      return tab
//    }
//  }
//  else this.logError('tab(), неверный параметр: ' + options)
//}


// имя последовательности обычно формируется из имени таблицы и имени поля через _
MongoStore.prototype.initSequence = function(name, quantity, cb) {
  if (this.sequenceCollection) {
    var self = this
    this.sequenceCollection.updateOne({_id: name}, {s: quantity}, 
        _.extend({upsert: true}, this.write_concern), function (err, res) {
      if (self.wasError(err, 'initSequence')) cb(err, false)
      else if (res && res.result.ok === 1) cb(null, true)
      else cb(null, false)
    })
  }
  else cb()
}


MongoStore.prototype.okSequence = function(name, quantity, cb) {
  if (this.sequenceCollection) {
//cl('in okSequence', name, quantity)
    var self = this
    this.sequenceCollection.findOne({_id: name}, {fields: {_id: 0}}, 
                                    function (err, res) {
//cl('in okSequence cb ', name, err, res)
      if (self.wasError(err, 'okSequence')) cb(err, null)
      else if (!res || (res.s === undefined) || res.s < quantity) cb(null, false)
      else cb(null, true)
    })
  }
  else cb()
}


MongoStore.prototype.getNextSequence = function(name, cb) {
  if (this.sequenceCollection) {
    var self = this
    this.sequenceCollection.findOneAndUpdate({_id: name}, {$inc: {s: 1}}, 
        {upsert: true, returnOriginal: false}, function (err, res) {
      if (self.wasError(err, 'getNextSequence')) cb(err, null)
      else if (res.ok && res.value) cb(null, res.value.s)
      else {
        self.logError('Неизвестная ошибка в getNextSequence, рез-т: ' + res)
        cb(null,null)
      }
    })
  }
  else cb()
} 


