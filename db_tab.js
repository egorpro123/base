var _ = require('underscore')
var async = require('async')
var dummy = require('./dummy')
var tab = require('./tab')
var valuesLib = require('../../utils/values')
var cl = require('../../utils/console_log').server_log


/*
  options:  
  см.также опции в Tab
*/

/**
* Таблица БД
* @class
*
* @param {Object} options настройки (см.также опции в Tab)
* @param {Object} [options.db|options.store] db открытое соединение с БД, а
*    store - Хранилище, имеет открытое соединение в store.db
* @param {Object[]|Array[]} fields - массив объектов описания полей 
*          или массив массивов в виде [['имя_поля','тип', длина, 'unique',...]],
*          для mongodb допускается не задавать все существующеие в БД поля, 
*          только присутсвующие в этих опциях (например контроль, индексы и т.п.)
*   [{name:.., 
*       type:.., // string,number,boolean,datetime
*                // (и для mongodb)id,array,object
*       len:..,  // в виде nnn или nnn.nn для чисел
*       unique: true // может отстуствовать
*       auto_inc: true // автоинкрементное поле, может отстуствовать
*       not_null: true // может отстуствовать, для строк - запрещает пустые строки
*     }]
*     сохранеяется в виде объекта this.fields = {имя_поля1: {}, имя_поля2: {}}
*     не исп. auto_rnd_inc: true // автоинкрементное поле, слева заполняется случайными числами
*                          // до длины len, может отстуствовать
*
* @param {Object[]|Array[]} indexes индексы в виде массива объектов   
*    [{
*      name: ,
*      fields: {name: order, ...}, // name имя поля, order 1 по возрастанию, -1 по убыванию
*      unique: true,
*      sparse: true //разряженный для mongodb
*      geo: true    //пространсв.коорд. для mongodb (geospatial 2dsphere),
*                   //не поддерживается по нескольких полям с некоординатными полями
*    },...]
*    или в сокр.виде в виде массива массивов 
*    [['имя_поля', fields в виде {name:order,...},'unique',...],...]
*    сохранеяется в виде объекта this.indexes = {имя_индекса1: {fields:{},...}, 
*       имя_индекса2: {fields:{},...}, ...}
*
* @param {Object[]} fieldsNamesConvertion переименовывание полей в БД, в виде
*    {
*      usedName: dbName,
*      ... 
*    }
*
* @param {string|string[]} primaryKey первичный ключ в виде 'имя_поля' или 
*    'имя_поля1, имя_поля2' или ['имя_поля1', 'имя_поля2', ...]
*    можно не указывать для поля с именем или типом id, создается автоматически
*    можно не указывать индексы по primaryKey, данные индексы создаются автоматически
*
*/

var DbTab = exports.Tab = function (options) {
//  mod.DbTab._super.constructor.call(this, options);     
  tab.Tab.call(this, options)
  this.set_options(options)
}

valuesLib.inherit(DbTab, tab.Tab)

DbTab.prototype.type = 'db_dummy_tab'

dummy.setMethodsUndefined(DbTab.prototype, ['max', 'min', 'fields', 'add_func',
  'set_func'])


DbTab.prototype.set_options = function (options) {
  valuesLib.ownDefaults(options, {
    db: undefined,
    store: undefined,
    fields: undefined,
    indexes: undefined,
    fieldsNamesConvertion: undefined,
    primaryKey: undefined
  })
  
  this.objectMulfunction = false
  if (!options.db && !options.store)
    this.setMulfunction('при создании не указана БД - options.db или options.store');
  else if (options.store) {
    this.store = options.store
    this.db = options.store.db
  }
  else this.db = options.db;
  
//cl('options',options)
  if (!options.fields) this.fields = {}
  else
    if (_.isArray(options.fields)) {
      var res = valuesLib.parseOptions(options.fields, [
        {name: 'type', values: 'string,number,boolean,datetime,id,array,object', 
           default: 'string'},
        {name: 'len', type: 'number'},
        {name: 'unique', equal: 'unique'},
        {name: 'auto_inc', equal: 'auto_inc'},
//        {name: 'auto_rnd_inc', equal: 'auto_rnd_inc'},
        {name: 'not_null', equal: 'not_null'}
      ])
//cl('res',res)
      this.fields = res.res
      if (!res.ok) this.setMulfunction('проблемы с определениями полей options.fields' +
                      res.errors)                 
    }
    else this.fields = options.fields;
  
  // индексы 
  if (options.indexes) {
    var flds
    var res = valuesLib.parseOptions(options.indexes, [
      {name: 'fields', type: 'object', required: true},
      {name: 'unique', equal: 'unique'},
      {name: 'sparse', equal: 'sparse'},
      {name: 'geo', equal: 'geo'},
    ])
    this.indexes = res.res
    if (!res.ok) {
      this.setMulfunction('Проблемы с определениями индексов options.indexes ' +
        res.errors)
    }
    if (this.type !== 'MongoTab') {
      for (var i in this.indexes) {
        flds = this.indexes[i].fields
        if (!valuesLib.propsSetted(this.fields, _.keys(flds))) {
          this.setMulfunction('Поле, указанное в индексе ' + i + 
                              ' отсутствует в options.fields')
          delete this.indexes[i]
        }
      }
    } 
    if (valuesLib.is_empty(this.indexes)) delete this.indexes
  }
  
  if (options.fieldsNamesConvertion) 
    this.fieldsNamesConvertion = options.fieldsNamesConvertion 
  
  if (this.fields){    
    var idCounter = 0
    _.each(this.fields, function(item){
      if (item.type === 'id') idCounter++
    })
    if (idCounter > 0)
      if (this.type !== 'MongoTab') 
        this.setMulfunction('Не допускается поле с типом id для таблицы типа ' + this.type)
//      else if (idCounter > 1) 
//        this.setMulfunction('Поле с типом id должно быть одно')
  }

  if (!options.primaryKey) {
    if (this.fields && this.fields.id) this.primaryKey = ['id']
//    if (idCounter > 0) {
//      var f = _.find(this.fields, function(item){
//        return item.type === 'id'
//      })
//      if (f) this.primaryKey = [f.name]
//    }
//    else {
//      var f = _.find(this.fields, function(item){
//        return item.name === 'id'
//      })
//      if (f) this.primaryKey = [f.name]
//    }
    
//      this.primaryKey = []
//    if (_.some(this.fields, function(f){return f.type === 'id'}))
//      this.logError('Если в fields указано поле с типом id ' + 
//                     'должен быть передан options.primaryKey')
  }
  else {
    // первичный ключ в виде '' или []
    //this.primaryKey должен быть массивом имен полей
    var opts
    if (_.isString(options.primaryKey)) 
      opts = options.primaryKey.replace(/\s/g,'').split(',')
    else if (_.isArray(options.primaryKey)) {
      if (!_.some(options.primaryKey, 
             function(item){ return !_.isString(item)})) opts = options.primaryKey
    }
    if (!opts) setMulfunction('Неверный формат параметра options.primaryKey: ' +
                              JSON.stringify(options.primaryKey))
    else if (!valuesLib.propsSetted(this.fields, opts)) 
      setMulfunction('Поле, указанное в options.primaryKey: ' + options.primaryKey +
                     '; отсутствует в options.fields')
    else this.primaryKey = opts;
  }
  
  if (this.objectMulfunction) {
    this.logError('ошибки про создании объекта, объект не может использоваться')
    dummy.setMethods(this, tab.tabMainMethods, function(){})
  }

  this.define_object_state()
//  
//  // объектные поля в виде {имя объекта: {объект описания полей}}
//  if (options.obj_fields) this.obj_fields = options.obj_fields;
}

DbTab.prototype.setMulfunction = function (msg) {
  this.logError(msg)
  this.objectMulfunction = true
}
  
DbTab.prototype.db_value_type = function (val) {
  if (val instanceof mongodb.ObjectId) return 'id'
  else return valuesLib.value_type(val)
}


// field - имя поля или объект настроек
// from_type - если известен тип поля можно передать для ускорения обработки
DbTab.prototype.to_db_type = function (val, field, from_type) {
  if (!field) return val
  if (_.isString(field)) 
    if (!this.fields[field]) return val
    else field = this.fields[field]
  if (field.type === 'id') {
    if (!from_type) from_type = valuesLib.value_type(val)
    if (from_type !== 'string') return val
    else
      try { return new mongodb.ObjectId(val) }
      catch (e) { return val }
  }
  else return valuesLib.to_type(val, field.type, from_type)
}

DbTab.prototype.define_object_state = function() {
  var f
  this.state = {}
  this.state.need_not_null_checking = false
  this.state.need_fields_incrementation = false
//  this.state.exist_id_type_fields = false
  for (var i in this.fields) {
    f = this.fields[i]
    if (f.not_null) this.state.need_not_null_checking = true
    if (f.auto_inc) this.state.need_fields_incrementation = true
//    if (f.auto_inc || f.auto_rnd_inc) this.state.need_fields_incrementation = true
//    if (f.type === 'id') this.state.exist_id_type_fields = true
  }
  if (!this.store) this.state.need_fields_incrementation = false
  if (this.fieldsNamesConvertion) this.state.needFieldsNamesConvertion = true
}


DbTab.prototype.set = function (id, obj, callback) {
  var self = this, check_res

  check_res = this.ok_for_save(obj)
  
  if (check_res.ok) this.set_func(id, obj, callback)
  else {
    var s = 'При записи объекта отсутствуют обязательные поля: ' + 
                      check_res.empties.join(', ')
    this.logError(s)
    callback(new Error(s), null)
  }
}

DbTab.prototype.ok_for_save = function (obj, options) {
  var empties = []
  
  options || (options = {})
  if (!options.no_check && this.state.need_not_null_checking)
    for (var i in this.fields)
      if (this.fields[i].not_null && valuesLib.is_empty(obj[i])) 
        empties.push(i)
//        ok_checking = false
//        err_str += ', ' + err_str
//        this.logError('При добавлении объекта отсутствует обязательное значение поля ' + i)
  
  return {
    ok: empties.length === 0,
    empties: empties 
  }
}


//DbTab.prototype.genRndIdByIncId = function(fld, id) {
//  if (this.fields[fld] && this.fields[fld].len) var maxl = this.fields[fld].len | 0
//  else var maxl = 9;
//  var l = valuesLib.countDigits(id)
//  if (l >= maxl) return id
//  else return Math.floor(Math.random() * Math.pow(10, maxl-l), 0) * Math.pow(10, l) + id
//}


//genRndIdByIncId('id', 1)
// var z = (Math.random() * Math.pow(10, maxl-l)); console.log(z, z|0)


// добавить запись
// осуществляет проверку, если не указано options.no_check
// делает автоинкрементацию полей

// options (не обязательное):
//   no_check - не делать проверку записи
//   no_change_id - не пересоздавать ИД, ИД должен быть создан заранее

DbTab.prototype.add = function (obj, options, callback) {
  
  var check_res
  var self = this
  if (callback === undefined) {
    callback = options
    options = {}
  }

//  проверка добавляемой записи
  check_res = this.ok_for_save(obj, options)
  
  if (check_res.ok) {
    var func = function(cb) {cb()}
// добавление автоинкрементных значений
    if (this.state.need_fields_incrementation) {
      var a = []
      for (var i in this.fields) {
//        if ((this.fields[i].auto_inc || this.fields[i].auto_rnd_inc) && 
        if (this.fields[i].auto_inc && 
            this.fields[i].type !== 'id' && 
            (!options.no_change_id || (this.primaryKey.indexOf(i) >= 0))) 
          a.push(i)
      }
      if (a.length >= 0)
        func = function(cb) {
          async.map(a, function(fld, cb) {
            self.store.getNextSequence(self.make_sequence_name(fld), 
                                         function(err, num){
              cb(err, {fld: fld, num: num})
            })
          }, function(err, results){
            if (self.wasError(err, 'add добавление документа')) cb() 
            else {
              for (var i in results) 
                obj[results[i].fld] = results[i].num
//                if (self.fields[results[i].fld].auto_rnd_inc)
//                  obj[results[i].fld] = self.genRndIdByIncId(results[i].fld,
//                                                             results[i].num)
//                else obj[results[i].fld] = results[i].num
              cb()
            }
          })
        }
    }
    
    func(function(){
      self.add_func(obj, function(err, res){
        self.wasError(err,'ошибка при добавлении записи: ' + JSON.stringify(obj))
        if (callback) callback(err, res)
      }, options)
    })
  }
  else {
    var s = 'При добавлении объекта отсутствуют обязательные поля: ' + 
                      check_res.empties.join(', ')
    this.logError(s)
    if (callback) callback(new Error(s), null)
  }
}

