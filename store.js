var dummy = require('./dummy')
var _ = require('underscore');
//var dummy = require('./dummy')
var valuesLib = require('../../utils/values')

var cl = require('../../utils/console_log').server_log

// в наследнике должна быть реализована open_func
/*
* @class
* @param {Object} options
* @param {Object} [options.databank] БД открыта в пуле БД (банке данных)
*/
var Store = exports.Store = function (options) {
  dummy.Dummy.call(this, options)
  _.extend(this, _.pick(options, 'databank', 'name_in_bank'))
}

valuesLib.inherit(Store, dummy.Dummy);

Store.prototype.caption = 'БД'

Store.prototype.type = 'dummy_store'

var dbMainMethods = ['open', 'close', 'getNextSequence', 'tab']

dummy.setMethodsUndefined(Store.prototype, dbMainMethods)


// при открытии БД должны быть вызвана данная функции с 
// open_func в виде function(callback), callback должен быть передан открывающей функции
// callback в виде function(err, database)
Store.prototype.open = function (callback) {
  if (this.db) { 
    this.log_warning('повторное открытие БД');
    if (callback) callback(null, this)
  }
  else 
    if (!this.open_func) {
      this.logError('Не определена функция открытия БД this.open_func')
      if (callback) callback()
    }
    else {
      var self = this
      this.open_func(function(err, database){
//cl("DB>JS", database.toString())
        if (!self.wasError(err, 'open') && database) {
          self.db = database
          if (self.on_open) self.on_open()
          callback(err, self)
        }
        else callback(err, null)
      })
    }
}

//Store.prototype._open = function (open_func, callback) {
//  if (this.db) { 
//    this.log_warning('повторное открытие БД');
//    if (callback) callback(null, this)
//  }
//  else {
//    var self = this
//    _.bind(open_func, this)(function(err, database){
//      if (!self.wasError(err, 'open')) self.db = database
//      if (self.on_open) self.on_open()
//      callback(err, self)
//    })
//  }
//}

Store.prototype.close = function (callback) {
  if (!this.db) { 
    this.log_warning('попытка закрыть неоткрытую БД');
    if (callback) callback(null, this)
  }
  else
    if (!this.close_func) {
      var s = 'Не определена функция закрытия БД this.close_func'
      this.logError(s)
      if (callback) callback(new Error(s), this)
    }
    else {
      var self = this
      this.close_func(function(err, res){
        if (!self.wasError(err, 'close')) delete self.db
        if (self.on_close) self.on_close()
        if (self.databank && self.name_in_bank) 
          self.databank.close_store(self.name_in_bank)
        if (callback) callback(err, self)
      })
    }
} 


//Store.prototype._close = function (close_func, callback) {
//  if (!this.db) { 
//    this.log_warning('попытка закрыть неоткрытую БД');
//    if (callback) callback(null, this)
//  }
//  else {
//    var self = this
//    _.bind(close_func, this)(function(err, res){
//      if (!self.wasError(err, 'close')) delete self.db
//      if (callback) callback(err, self)
//    })
//  }
//}
//

Store.prototype.opened = function() {
  return !!this.db
}


