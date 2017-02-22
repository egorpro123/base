'use strict';

var _ = require('underscore')
var valuesLib = require("../../utils/values")
var dummy = require("./dummy")
var mongoStore = require("./mongo_store")
var log = require('winston')
//var async = require('async')
var config = require('./db_config')


var cl = require('../../utils/console_log').server_log;

//var mod = exports;

//var noname_tab_counter = 0;
//var dbs = {};



/*
---------------------------------------------------------------------------------
определения объектов
---------------------------------------------------------------------------------
*/

//var def_class = function () {}
//
//mod.def_class = def_class;



//---------------------------------------------------------------------------------

/*
коннект для экспериментов

var mongodb = require('mongodb');
var db,test,res;
mongodb.MongoClient.connect("mongodb://localhost:27017/test", 
      function (err, database) { 
        db = database;
        test = db.collection('test');
      }
)
process.on('uncaughtException', function(err) {
  console.log('Caught exception: ' + err);
});
var log = function(e, r){if (e) console.log("ERROR> " + e); else {
  res = r
  console.log("RESULT> ", r);
}}
var id = function(id) {return new mongodb.ObjectID(id)}
*/

//---------------------------------------------------------------------------------


//---------------------------------------------------------------------------------

// options: name, host, port, connection_pool




//---------------------------------------------------------------------------------

var SUPPORTED_DB_TYPES = 'mongodb,memory'

var Databank = exports.Databank = function (options) {
  options || (options = {})
  if (!options.name) options.name = 'main'
  dummy.Dummy.call(this, options)  
  this.stores = {}
  this.currentStore = null
}

valuesLib.inherit(Databank, dummy.Dummy);

Databank.prototype.caption = 'Набор БД'

Databank.prototype.type = 'databank'

//var databank_main_methods = ['add', 'open', 'open_main', 'store', 'tab',
//                            'close', 'close_main']
//
//set_methods_undefined(Databank.prototype, databank_main_methods)



/*
добавить БД со следующими параметрами

!возбуждает ошибки

options = {
  type: 'mongodb', // обязательное
  host: '', // обязательное
  port: '', // обязательное
  name: '', // имя БД
  name_in_bank: '', // наименование в банке данных
  user: '',
  password: '',
  dont_open: 'не открывать БД при добавлении'
}
*/

//Databank.prototype.add_store = function (options, callback) {
//  options || (options = {})
//  var s = valuesLib.propsSetted(options, 'type,host,port,name', true);
//  var errs = this.errors_counter
//  if (!options.name_in_bank) options.name_in_bank = options.name
//  if (s !== "")
//    this.logError('При добавлении БД ' + options.name_in_bank +
//                   ' не указаны обязательные параметры: ' + s)    
//  if (!valuesLib.in_comma_str(SUPPORTED_DB_TYPES, options.type)) 
//    this.logError('Неизвестный тип БД: ' + options.type)
//  if (this.errors_counter === errs) {
//    if (this.stores[options.name_in_bank]) {
//      this.logError('Невозможно добавить БД: ' + options.name_in_bank + 
//        '. БД с таким именем уже есть.')
//    }
//    else {
//      this.stores[options.name_in_bank] = {options: options};
//      if (!options.dont_open) 
//        this.openStore(options.name_in_bank, callback)
//    }
//  }
//  else if (callback) callback()
//};


Databank.prototype.okStoreOptions = function (options) {
  var ok = true
  var s = valuesLib.propsSetted(options, 'type,host,port,name', true);
  
  if (!options.name_in_bank) options.name_in_bank = options.name
  if (s !== "") {
    this.logError('При открытии БД ' + options.name_in_bank +
                   ' не указаны обязательные параметры: ' + s)    
    ok = false
  }
  if (!valuesLib.in_comma_str(SUPPORTED_DB_TYPES, options.type)) {
//   cl(SUPPORTED_DB_TYPES,options.type)
    this.logError('Неизвестный тип БД: ' + options.type)
    ok = false
  }
  if (ok) {
    if (this.stores[options.name_in_bank]) {
      this.logError('Невозможно открыть БД: ' + options.name_in_bank + 
        '. БД с таким именем уже открыта.')
      ok = false
    }
  }
  return ok
};


Databank.prototype.store_opened = function (name){
  return this.stores[name] && this.stores[name].opened()
}


Databank.prototype.openStore = function (options, callback) {
  var base = null, ok = false, self = this
  options || (options = {})
  ok = this.okStoreOptions(options)
  if (ok) {
    if (!options.name_in_bank) options.name_in_bank = options.name
    if (self.stores[options.name_in_bank]) {
      var err_str = 'Уже открыта БД с таким именем: ' + options.name_in_bank 
      this.logError(err_str)
      if (callback) callback(new Error(err_str), null)
    } else {
      options.databank = this
      base = new mongoStore.Store(options)
      base.open(function(err, res) {
        if (!err && res) {
          self.currentStore = base
          self.stores[options.name_in_bank] = base
        }
        if (callback) callback(err, res)
      })
    }
  }
  else if (callback) callback(null, null)
}

//Databank.prototype.openStore = function (name, callback) {
//  if (!this.stores[name] || (!this.stores[name].options)) {
//    this.error_log('При попытке открытия БД не найдены настройки:' + name)
//    if (callback) callback()
//  }
//  else {  
//    var base = this.stores[name]
//    if (!base.store && base.options.type === 'mongodb')
//      base.store = new MongoStore(base.options)
//    if (!base.store.opened()) {
//      var self = this
//      base.store.open(function(err, res) {
//        if (!err && res) self.currentStore = base.store
//        if (callback) callback(err, res)
//      })
//    }
//    else {
//      this.currentStore = base.store
//      if (callback) callback(null, base.store)
//    }
//  }
//}


Databank.prototype.close_store = function (name, callback) {
  if (!this.stores[name]) {
    this.logError('При закрытии не найдена БД:' + name)
    if (callback) callback()
  } 
  else if (!this.stores[name].opened()) {
    delete this.stores[name]
//    this.log_warning('Попытка закрытия неоткрытой БД:' + name)
    if (callback) callback(null, true)
  } 
  else {
    var self = this
    this.stores[name].close(function(){
      if (self.currentStore === self.stores[name]) 
        self.currentStore = null
      delete self.stores[name]
      if (callback) callback(null, true)
    })
//    this.stores[name].store.close(function(){
//      var s = self.stores[name].store
//      if (self.currentStore === s) self.currentStore = null
//      delete self.stores[name].store
//      if (callback) callback(null, s)
//    })
  }
}

  
Databank.prototype.openMainStore = function (callback) {
  var s = valuesLib.propsSetted(config.db, 'type,host,port,name', true);
  if (s === '') {
    var o = _.extend({}, config.db)
    o.name_in_bank = '_main_db'
    var self = this
    this.openStore(o, function(err, res){
      if (err) {
        self.logError('При открытии главной БД произошла ошибка ' + err.message)
        res = null;
      }
      if (!res) self.logError('Главная БД не открыта.')
      else self.main_db = res
      if (callback) callback(err, res)
    })
  }
  else this.error_log('Главная БД не открыта. В файле config.json в поле db '+
          'не установлены обязательные свойства: ' + s)
}


Databank.prototype.closeMainStore = function (callback) {
  var self = this
  this.close_store('_main_db', function(err, res){
    if (!res) {
      var s = 'Главная БД не закрыта'
      self.logError(s)
      if (callback) callback(new Error(s))
    }
    else {
      var d = self.main_db
      delete self.main_db
      if (callback) callback(null, d)
    }
  })
}

// возвращает хранилище
Databank.prototype.store = function (name) {
  if (this.stores[name]) 
    return this.stores[name]
}


// вызывается tab(store, tab) или tab(tab)
Databank.prototype.tab = function(store_name, tab) {
  var store
  if (arguments.length = 1 && this.currentStore) {
    tab = store_name
    store = currentStore
  }
  else store = this.store(store_name)
  if (store) return store.tab(tab)
}

























