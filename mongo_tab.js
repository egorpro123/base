var _ = require('underscore');
//var dummy = require('./dummy')
var dbTab = require('./db_tab')
var valuesLib = require('../../utils/values')
var mongodb = require('mongodb')
var async = require('async')

var cl = require('../../utils/console_log').server_log
var config = require('./db_config')


// поле id, если оно одно, для хранения в БД переименовывается в _id
// по умолчанию _id считается стандартным mongodb идентификатором
// если id или _id отсутствует, то поле _id автоматически добавляется

// options: name, db (подключение)
//   см.также опции в DbTab
//
//   write_safety:  // параметры сохрания данных на сервере
//     none, no_guarantee, journal (default), one_replica, majority_replica
//   do_checking: false // сделать проверку структуры таблицы в базе 
//      и узнать начальное количество записей (при этом должен быть передан callbak)
//   onCreateDone: null // колбек должен быть передан при проведении проверки do_checking=true
//        формат - function(err, true/false)

// удалено:
//   no_normalize_id: true/false //не делать проверку ИД при операциях по ИД (get,set,...)
//         // по умолчанию по первичн.ключу из одного поля - true, из нескольких - false


var KEY_FIELDS_DELIMITER = '|'

/**
 * Таблица (коллекция) mongodb
 * @class
 */
// * @namespace 
var MongoTab = exports.Tab = function (options) {
  dbTab.Tab.call(this, options);
  valuesLib.ownDefaults(options, {
    do_checking: undefined,
    write_safety: undefined,
    onCreateDone: undefined
  })
  if (this.db) this.tab = this.db.collection(this.name);
  this.checkFieldsAndPrimaryKey()
  this.checkOrCreateUnigueFieldIndex()
  if (this.db) this.define_write_concern(options)
  this.clearQueryRules()
  if (options.do_checking) {
    if (!options.onCreateDone) 
      this.logError('При передаче опции do_checking должна быть передана функция ' +
                    'onCreateDone')
    else this.check_db_structures(options.onCreateDone)
  }
  else if (options.onCreateDone) options.onCreateDone(null, true) 
//  this.selector_options = {}
//  if (options.no_normalize_id === undefined) {
//    if (this.primaryKey.length <= 1) this.no_normalize_id = true
//    else this.no_normalize_id = false
//  }
//  else this.no_normalize_id = options.no_normalize_id
}

valuesLib.inherit(MongoTab, dbTab.Tab);


MongoTab.prototype.type = 'MongoTab'

/**
* очистить условия запросов
* @function
*/
MongoTab.prototype.clearQueryRules = function () {
//  this.selectors = {}
//  this.queryOptions = {}
//  Tab.prototype.clearQueryRules.call(this)
  this.querySelector = {}
  this.currentSelector = this.querySelector
  this.queryOptions = {}
}

MongoTab.prototype.isNoQueryConditions = function () {
  return valuesLib.is_empty(this.querySelector) &&
    valuesLib.is_empty(this.currentSelector) &&
    valuesLib.is_empty(this.queryOptions)
}


MongoTab.prototype.setSelectorCondition = function(field, condition, value) {
  if (this.state.needMongoIdConvertion && 
      (this.mongoIdFields.indexOf(field) > -1)) {
    value = this.convert2MongoObjectID(value)
  }
  if (this.state.needFieldsNamesConvertion &&
    this.fieldsNamesConvertion.hasOwnProperty(field)) {
    field = this.fieldsNamesConvertion[field]
  }      
  
  if (!this.currentSelector.hasOwnProperty(field)) 
    this.currentSelector[field] = {}
  this.currentSelector[field][condition] = value
}


MongoTab.prototype.setSelectorValue = function(field, value) {
    if (!this.currentSelector.hasOwnProperty(field)) 
      this.currentSelector[field] = {}
    this.currentSelector[field] = value
}


MongoTab.prototype.setQueryOptionsCondition = function(field, condition, value) {
    if (!this.queryOptions.hasOwnProperty(condition)) 
      this.queryOptions[condition] = {}
    if (this.fieldsNamesConvertion && 
        this.fieldsNamesConvertion.hasOwnProperty(field))
      this.queryOptions[condition][this.fieldsNamesConvertion[field]] = value
    else this.queryOptions[condition][field] = value
}

_.each(['eq', 'gt', 'gte', 'lt', 'lte', 'ne'], function(func) {
  MongoTab.prototype[func] = function(field, value) {
    this.setSelectorCondition(field, '$' + func, value)
    return this
  };
});

// с учетом регистра - options = {case: true}
MongoTab.prototype.like = function(field, value, options) {
  this.setSelectorValue(field, 
    valuesLib.make_like_regexp(value, options && options.case))
  return this
};

_.each(['in', 'notin', 'inall'], function(func) {
  MongoTab.prototype[func] = function(field, value) {
    if (!_.isArray(value)) this.logError('Параметр значения функции ' + func + 
                                     ' должен быть массивом')
    else {
      if (func == 'in') this.setSelectorCondition(field, '$in', value)
      else if (func == 'notin') this.setSelectorCondition(field, '$nin', value)
      else this.setSelectorCondition(field, '$all', value)
    }    
    return this
  }
})


MongoTab.prototype.max = function(field, value) {
  this.setQueryOptionsCondition(field, 'sort', -1)
  return this
}


MongoTab.prototype.min = function(field, value) {
  this.setQueryOptionsCondition(field, 'sort', 1)
  return this
}

// sort('имя_поля'[, 1|-1(по умолчанию 1)]) 
MongoTab.prototype.sort = function(field, value) {
  if (value !== undefined && value != 1 && value != -1) 
    this.logError('Дополнительный параметр функции sort может быть 1 или -1')
  else this.setQueryOptionsCondition(field, 'sort', !!value ? value : 1)
  return this
}

// leave(fields,options) указать нужные поля, при options {exclude: true} - ненужные поля 
MongoTab.prototype.leave = function(fields, options) {
  var ok = true
  if (_.isString(fields)) fields = valuesLib.comma_str2array(fields)
  else if (!_.isArray(fields)) {
    this.logError('Список полей функции leave должен быть строкой или массивом')
    ok = false
  }
  if (ok) {
    var p = options && options.exclude ? 0 : 1
    for (var i in fields) if (fields.hasOwnProperty(i))
      this.setQueryOptionsCondition(fields[i], 'fields', p)
//cl('this.queryOptions', this.queryOptions)
  }
  return this
}

_.each(['limit', 'skip'], function(func) {
  MongoTab.prototype[func] = function(value) {
    this.queryOptions[func] = value
    return this
  };
})



MongoTab.prototype.or = function() {
  if (this.querySelector.$or) {
    this.querySelector.$or.push(this.currentSelector)
  }
  else this.querySelector = {$or: [this.currentSelector]}
  this.currentSelector = {}
}

MongoTab.prototype.makeQueryObjects = function() {
  if (this.querySelector.$or) this.querySelector.$or.push(this.currentSelector)
}

//_.each(['eq', 'gt', 'gte', 'lt', 'lte', 'ne'], function(func) {
//  MongoTab.prototype[func] = function(fld_name, value, options) {
//    if (this.id_convertion && this.primaryKey.length > 1) {
//      var fit = this.fit_primary_key(fld_name)
//      if (fit === 'partially') {
//        this.logError(func +'(): Недопустимы операции над отдельными полями '+
//          'ключевого индекса из нескольких полей или если поля перечислены в ином порядке: ' +
//          fld_name)
//        return this
//      }
//      else if (fit === 'full') {
//        fld_name = '_id'
//        if (_.isString(value)) value = value.replace(/,/g,'|')
//        else if (_.isArray(value)) value = value.join('|')
//      }
//    }
//    var o, a, v = valuesLib.value_type(fld_name), o2
//    if (v === 'string' || v === 'array') {
//      if (v === 'string') {
//        a = valuesLib.comma_str2array(fld_name)
//        if (a.length === 0) return this
//        else if (a.length === 1) {
//          o = {}
//          o[fld_name] = value
//        }
//        else {
//          if (_.isString(value)) o = _.object(a, valuesLib.comma_str2array(value))
//          else o = _.object(a, value)
//          this.normalize_values_types(o, true)
//        }
//      }
//      else o = _.object(a, value)
//    }
//    else if (v === 'object') o = fld_name
//    else return this
//    
//    
//    this.rules[this.current_rule].push({
//      type: func,
//      fld: o
//    });
//    return this;
//  };
//});

  
// leave(fields,options) указать нужные поля, при options {exclude: true} - ненужные поля 
// sort('имя_поля'[, 1|-1(по умолчанию 1)]) 
//_.each(['max', 'min', 'leave', 'sort'], function(func) {
//  MongoTab.prototype[func] = function(fld_name, value, options) {
//    this.rules[this.current_rule].push({
//      type: func,
////      fld: ("" + fld_name).trim(),
//      fld: fld_name,
//      val: value,
//      options: options
//    });
//    return this;
//  };
//});

//_.each(['limit', 'skip'], function(func) {
//  MongoTab.prototype[func] = function(fld_name, value, options) {
//    this.rules[this.current_rule].push({
//      type: func,
//      val: fld_name,
//      options: value
//    });
//    return this;
//  };
//});

// проверяет поля на совпадение с полями ключевого индекса
//MongoTab.prototype.fit_primary_key = function (key) {
//  if (this.primaryKey.length === 0) return 'none'
//  else {
//    var t = valuesLib.value_type(key), a
//    if (t === 'string') a = valuesLib.comma_str2array(key)
//    else if (t === 'array') a = key
//    else if (t === 'object') a = key.keys()
//    return 'none'
//    
//    var ok = true
//    var was = false
//    for (var i in a) {
//      if (a[i] !== this.primaryKey[i]) ok = false
//      if (this.primaryKey.indexOf(a[i]) >= 0) was = true
//    }
//    if (ok) return 'full'
//    else if (was) return 'partially'
//    else return 'none'
//  }
//}

// можно ускорить обработку создав объект с методами
//MongoTab.prototype.makeQueryObjects = function () {
//  var selector, or_rule, or_rule_ind, rule, rule_ind, sels = [], o
//  
////cl('IN makeQueryObjects')
//  this.querySelector = {}
//  this.queryOptions = {}
//  for (or_rule_ind in this.rules) {
//    or_rule = this.rules[or_rule_ind]
//    if (valuesLib.is_empty(or_rule)) continue
//    selector = {}
//    for (rule_ind in or_rule) {
//      rule = or_rule[rule_ind]
//      if (['eq', 'gt', 'gte', 'lt', 'lte', 'ne'].indexOf(rule.type) >= 0)
//        for (var i in rule.fld) {
//          o = {}
//          o['$' + rule.type] = rule.fld[i]
//          if (!selector[i]) selector[i] = o
//          else _.extend(selector[i], o)
//        }
//      
//      else if (rule.type === 'like') {
//        if (rule.options && rule.options.case) 
//          selector[rule.fld] = valuesLib.make_like_regexp(rule.val, true)
//        else selector[rule.fld] = valuesLib.make_like_regexp(rule.val)
//      }
//
//      else if (rule.type === 'in')
//        selector[rule.fld] = { $in: rule.val }
//      else if (rule.type === 'notin')
//        selector[rule.fld] = { $nin: rule.val }
//      else if (rule.type === 'inall')
//        selector[rule.fld] = { $all: rule.val }
//        
//      else if (rule.type === 'max' || rule.type === 'min') {
//        if (!this.queryOptions.sort) this.queryOptions.sort = {}
//        this.queryOptions.sort[rule.fld] = rule.type === 'max' ? -1 : 1
//      }
//      
//      else if (rule.type === 'sort') {
//        var o = {}
//        if (valuesLib.is_empty(rule.val)) o[rule.fld] = 1
//        else o[rule.fld] = rule.val
//        this.id2mongo_id(o)
//        if (!this.queryOptions.sort) this.queryOptions.sort = {}
//        this.queryOptions.sort = _.extend(this.queryOptions.sort, o)
//      }
//      
//      else if (rule.type === 'leave') {
//        var p, o = {}
//        rule.options = rule.val
//        if (rule.options && rule.options.exclude) p = 0
//        else p = 1
//        if (!_.isArray(rule.fld)) o[rule.fld] = p
//        else for (var i in rule.fld) o[rule.fld[i]] = p
//        this.queryOptions.fields = o
//      }
//      
//      else if (rule.type === 'limit' || rule.type === 'skip') {
//        this.queryOptions[rule.type] = rule.val
//      }
//      else this.logError('Неизвестное условие запроса: ' + rule.type);
//    }
//cl('BEFORE', selector)
//    if (this.id_convertion) this.id2mongo_id(selector)
//cl('AFTER', selector)
//    sels.push(selector)
//  }
//  if (sels.length = 1) this.querySelector = sels[0]
//  else this.querySelector = {$or: sels}
//  if (this.id_convertion && !valuesLib.is_empty(this.queryOptions)) {
//    if (this.queryOptions.fields) 
//      this.id2mongo_id(this.queryOptions.fields, true)
//    if (this.queryOptions.sort) 
//      this.id2mongo_id(this.queryOptions.sort, true)
//  }
////cl('go out')
//}


/**
* возвращает одну запись, если не найдет вернет null. В основном используется в цепочке query()
* @function
* 
*/
MongoTab.prototype.one = function (callback) {
  if (this.isNoQueryConditions()) { 
//cl('_.toArray(arguments)', _.toArray(arguments))
    this.get.apply(this, _.toArray(arguments)) 
  } else {
    var self = this
    this.makeQueryObjects()
    this.tab.findOne(this.querySelector, this.queryOptions, function(err, res){
      self.clearQueryRules()
      if (!valuesLib.is_empty(res)) self.convertFieldsFromDatabase(res)
//      if (self.id_convertion && !valuesLib.is_empty(res)) self.mongo_id2id(res)
//      if (self.id_convertion && !valuesLib.is_empty(res)) self.mongo_id2id(res)
      if (err) self.logError('one(): ошибка при БД запросе - '+ err.message)
      callback(err, res)
    })
  }
}


//
/**
* Запрос к БД после задания условий (функции eq(),sort(),...) с возвращением всех записей
* при этом условия запроса очищаются, если не указана опция no_clear_conditions.
* без условий получайте только небольшие по объему таблицы (<1000 записей)
* 
* @param {Object} [options]
* @param {boolean} [options.no_clear_conditions=false] не очищать условия запроса
* @param {function} callback колбэк (err, result)
*/
MongoTab.prototype.all = function (options, callback) {
  if (!callback) {
    callback = options
    options = {}
  }
  
  var self = this
  if (this.isNoQueryConditions()) {
    if (this.initial_recs_count && this.initial_recs_count > 1000) {
      this.logError('Будет отвергнута попытка прочитать больше 1000 записей без условий')
      callback(null, []) 
      return
    }
    this.querySelector = {}
    this.queryOptions = {}
  }
  else this.makeQueryObjects()
//cl('all',this.querySelector, this.queryOptions) 
  this.tab.find(this.querySelector, this.queryOptions).toArray(function(err, res){
    if (!options.no_clear_conditions) self.clearQueryRules()
    if (self.state.needFieldsNamesConvertion && !valuesLib.is_empty(res)) 
      _.each(res, function(r){ self.convertFieldsFromDatabase(r) })
//      _.each(res, function(r){ self.mongo_id2id(r) })
    callback(err, res)
  })
}


/**
* Запрос к БД после задания условий (функции eq(),sort(),...) с возвращением всех записей
* при этом чтение происходит партиями
* 
* @param {Object} [options]
* @param {boolean} [options.no_clear_conditions=false] не очищать условия запроса
* @param {function} callback колбэк (ended, result)
*/
MongoTab.prototype.stream = function (options, callback) {
  if (!callback) {
    callback = options
    options = {}
  }
  
  var self = this
  var buf = []
  this.makeQueryObjects()
  var cursor = this.tab.find(this.querySelector, this.queryOptions)
  if (options.batch_size) cursor.batchSize(options.batch_size)
  var stream = cursor.stream()
  if (!options.no_clear_conditions) self.clearQueryRules()
  
  stream.on('end', function() {
    callback(true, null)
  });

  stream.on('data', function(data) {
    self.convertFieldsFromDatabase(data)
//    if (self.id_convertion) self.mongo_id2id(data)
    callback(false, data)
  });  
  
}

//  var func = function(){
//cl('cursor', cursor) 
//    cursor.hasNext(function(err, ok){ 
//      if (err || !ok) callback(err, true, null)
//      else
//        cursor.next(function(err, res) {
//          if (err) callback(err, true, res)
//          else {
//            if (self.id_convertion && !valuesLib.is_empty(res)) 
//              res.forEach(function(r){ self.mongo_id2id(r) })
//  //            _.each(res, function(r){ self.mongo_id2id(r) })
//            callback(err, false, res)
//            process.nextTick(func)
//          }        
//        })
//    })
//  }  
//  func()
//}

//cursor = db.col.find() // Get everything!
//
//while(cursor.hasNext()) {
//    /* This will use the documents already fetched and if it runs out of documents in it's local batch it will fetch another X of them from the server (where X is batchSize). */
//    document = cursor.next();
//
//    // Do your magic here
//}

//MongoTab.prototype.all = function (callback) {
//  if (this.isNoQueryConditions()) {
//    this.tab.find().toArray(function(e, r){
//      if (this.wasError(e, 'all')) callback(e);
//      else {
//        if (this.id_convertion)
//          _.each(r, function(v){
//            this.mongo_id2id(v)
//          })
//        callback(null, r);
//      }
//    });
//  }
//}

//MongoTab.prototype.all = function (callback) {
//  if (this.isNoQueryConditions()) {callback()}
//  else {
//    var self = this
//    this.makeQueryObjects()
//    this.tab.find(this.querySelector, this.queryOptions).toArray(function(err, res){
//      self.clearQueryRules()
//      callback(err, res)
//    })
//  }
//}


//mongo_tab.max_value = function (field_name, cb) {
//  var o = {}
//  o[field_name] = -1
//  this.tab.find().sort(o).limit(1).next(cb)
//}



MongoTab.prototype.define_write_concern = function (options) {
  if (!options.write_safety || options.write_safety === this.db.write_safety)
    this.write_concern = this.db.write_concern
  else this.db.define_write_concern.call(this, options)
}


MongoTab.prototype.equal_indexes = function (def, spec) {
  var ok
  if (def.geo) ok = valuesLib.equalKeys(def.fields, spec.key) &&
    spec['2dsphereIndexVersion']
  else ok = valuesLib.equal_values(def.fields, spec.key) 
  return ok && valuesLib.equal_values(_.pick(def, 'unique', 'sparse'), 
                            _.pick(spec, 'unique', 'sparse'))
}


// callback(err, res), res = {ok: true/false/null, 
//     bads: [{name:'', state:'noexist'/'othername'/'extra', spec:...}]}
MongoTab.prototype.ok_indexes = function (callback) {
  if (this.indexes) {
    this.getCurrentIndexes(_.bind(function(err, specs){
      if (err) { 
        callback(null, {ok: null}); 
        return 
      }
      var ind, inds = [], from_spec, self = this
      for (var i in this.indexes) {
        ind = this.indexes[i]
        from_spec = _.find(specs, function(item){ 
          return self.equal_indexes(ind, item)
        })
        if (!from_spec) inds.push({
          name: i, 
          state: 'noexist'
        })
        else if (i !== from_spec.name) inds.push({
          name: i, 
          state: 'othername'
        })
      }
      for (var i in specs)
        if (!_.some(this.indexes, function(item){
              return self.equal_indexes(item, specs[i])
            }))
          inds.push({name: specs[i].name, state: 'extra', spec: specs[i]})
      callback(null, {ok: valuesLib.is_empty(inds), bads: inds})
    }, this))
  }
  else callback(null, {ok: true})
}



// callback(err, res), res = {ok: true/false/null, 
//                            bads: [{field: имя_поля, amount: число_кот.должно_быть}]}
MongoTab.prototype.ok_sequences = function (callback) {
//if (dolog) cl('in ok_sequences fields: ' + JSON.stringify(this.fields))
  if (!this.store || !_.find(this.fields, function(fld){ 
    return fld.auto_inc && fld.type !== 'id'
  })) callback(null, {ok: true})
  else {
    var self = this
    this.count(_.bind(function(err, amount){
      var flds = [], fld
      for (var i in this.fields) {
        fld = this.fields[i]
        if (fld.auto_inc && fld.type !== 'id') {
          if (amount > 1000 && (!this.indexes || !_.find(this.indexes, function(ind){ 
            return ind.fields[0][i]
          }))) this.log_warning('Возможно замедление в работе - при проверке посл-ти ' +  
              ' по полю ' + i +' будет произведен поиск макс.значения при отсутствии индекcа')
          flds.push(i)
        }
      }
//cl('ok_sequences flds', flds)      
      async.map(flds, function(fld_name, cb) {
        self.query().max(fld_name).leave(fld_name).one(function(err, doc){
          if (err) cb(err)
          else {
            var n
            if (!doc || !doc[fld_name]) n = 0
            else n = doc[fld_name]
            self.store.okSequence(self.make_sequence_name(fld_name), n, 
                                   function(e, r){
              cb(e, {ok: r, amount: n, fld: fld_name})
            })
          }
        })
      }, function(err, results){
//cl('ok_sequences results:', results)      
        if (err) callback(err, {ok:null})
        else {
          var a = []
          for (var i in results)
            if (!results[i].ok) {
              a.push({
                field: results[i].fld,
                amount: results[i].amount
              })
            }
          if (valuesLib.is_empty(a)) callback(err, {ok: true})
          else callback(err, {ok: false, bads: a})
        }
      })
    }, this))
  }
}


MongoTab.prototype.make_sequence_name = function (field_name) {
  return this.name + '_' + field_name
}


/**
* Cоздание последовательностей
* 
* @param {Array} описание последовательностей в виде [описание, ....] если не указано запустится 
* проверка последовательностей ok_sequences
* описание в виде {ok: false, bads: [{field, amount - начальное значение},], }
*/
MongoTab.prototype.create_sequences = function (bads, callback) {
  var func, self = this
  if (!bads) func = _.bind(this.ok_sequences, this)
  else func = function(cb){cb(null, {ok: false, bads: bads})}
  func(function(err, res){
    
    async.map(res.bads, function(seq, cb) {
      self.store.initSequence(self.make_sequence_name(seq.field), seq.amount, 
                               function(err, res){
        cb(err, {field: seq.name, ok: res})
      })
    }, function(err, results){
      var a = [], r
      for (var i in results) {
        r = results[i]
        if (!r.ok) a.push(r.field)
      }
      if (!valuesLib.is_empty(a)) 
        self.logError('Не создались последовательности: ' + a.join(', '))
      callback(err, valuesLib.is_empty(a))
    })              
  })
}



// callback(err, res), res = {ok: true/false/null, 
//                    bads: [{name:'', state:'noexist'/'bad'/'extra', spec:...}]}
MongoTab.prototype.check_db_structures = function (callback) {
  var ind, self = this
  this.okDbStructures(function (err, res){
    if (err) { if (callback) callback(err, null); return}
//cl('okDbStructures',res)
    if (res.hasOwnProperty('tab') && res.tab.ok === false)
      self.create(callback)
    else {
//cl('check_db_structures', res)
      if (err || res === true) callback(err, res)
      else self.make_db_structures(res, callback)
    }
  })
}

MongoTab.prototype.make_db_structures = function (res, callback) {
  var ind, self = this
  var o = {}, s = '', gen_func = function(func, name) {
    return function(cb) {
      var f = _.bind(func, self);
      f(name, cb)
    }
  }
  if (!callback) callback = function(err, res) {}

  if (res.indexes.ok === false) 
    for (var i in res.indexes.bads) {
      ind = res.indexes.bads[i]
      if (ind.state === 'noexist')
        o['index_' + ind.name] = gen_func(self.create_index, ind.name)
      else if (ind.state === 'extra')
        o['index_' + ind.name] = gen_func(self.drop_index, ind.name)
      else if (ind.state === 'othername')
        self.log_warning('индекс ' + ind.name + ' имеет другое имя в БД')
    }
  if (res.sequences.ok === false && self.store)
    for (var i in res.sequences.bads) {
      ind = res.sequences.bads[i]
      o['seq_' + ind.field] = (function(name, quantity){
        return function(cb) {
          self.store.initSequence(name, quantity, cb)
        }
      })(self.make_sequence_name(ind.field), ind.amount)
    }
//cl('check_db', res, o)
  if (res.indexes.ok && res.sequences.ok) callback(null, true)
  else if (valuesLib.is_empty(o)) {
    self.logError('check_db_structures: какие то проблемы при проверке, результат: ' +
                  JSON.stringify(res))
    callback(null, false)
  }
  else {
    async.parallel(o, function(err, result){
      var s = '', a
      for (var i in result) {
        if (!result[i]) {
          a = i.split('_')
          if (a[0] === 'index') s += ', индекса ' + a[i]
          else s += ', последовательности ' + a[i]
        }
      }
      if (s === '') callback(null, true)
      else {
        self.logError('возникли проблемы при создании структур таблицы: ' + s.slice(2))
        callback(true, false)
      }
    })
  }
}


// callback(err, res), res = {
//   ok: true/false,
//   tab: {ok:...},
//   indexes: {ok:...,bads:...}, 
//   sequences: {ok:...,bads:...}
// }
MongoTab.prototype.okDbStructures = function (callback) {
  var self = this
  self.exist(function(err, ok){
    if (err) { callback(err, null); return}
    if (ok) {
      var o = {}
      o.indexes = _.bind(self.ok_indexes, self)
      o.sequences = _.bind(self.ok_sequences, self)
      async.parallel(o, function(err, r){
        var o = {
          tab: {ok: true}
        }
        self.wasError(err, 'okDbStructures')
        o.ok = r.indexes.ok && r.sequences.ok
        _.extend(o, r)
        callback(err, o)
      })
    }
    else callback(false, {
      ok: false,
      tab: {ok: false}
    })
  })
}

//MongoTab.prototype.okDbStructures = function (callback) {
//  var self = this
//  var func = function(cb){
//    self.exist(function(err, ok){
//      if (ok) cb(err, ok)
//      else self.create(cb)
//    })
//  }
//  
//  func(function(err, res){ 
//    if (err) { callback(err,res); return}
//    var o = {}
//    o.indexes = _.bind(self.ok_indexes, self)
//    o.sequences = _.bind(self.ok_sequences, self)
//    async.parallel(o, function(err, r){
//      var o = {}
//      self.wasError(err, 'okDbStructures')
//      o.ok = r.indexes.ok && r.sequences.ok
//      _.extend(o, r)
//      callback(err, o)
//    })
//  })
//}


// возвращает индексы без стандартного по _id
MongoTab.prototype.getCurrentIndexes = function (func) {
  var self = this
  this.tab.indexes(function(err, res) {
    self.wasError(err, 'getCurrentIndexes')
    var a = [], ind
    for (var i in res) if (res.hasOwnProperty(i)) {
      ind = res[i]
      if (!_.isEqual(ind.key, { _id: 1 })) a.push(ind)
    }
    func(err, a)
  })
}

MongoTab.prototype.convert2MongoObjectID = function (val) {
  if (val instanceof mongodb.ObjectId) return val
  else if (_.isString(val)) {
    try{
      return new mongodb.ObjectId(val)
    } catch(e) {
      this.logError('Значение не может быть преобразовано в идентификатор монго - ' + val)
    }
  }
  else this.logError('Значение не может быть преобразовано в идентификатор монго - ' + val)
}


// не отрабатывает множественные индексы в виде строки через зпт
MongoTab.prototype.makeIdSelector = function(val) {
  if (valuesLib.is_object(val)) return val
  
  if (this.hasOwnProperty('primaryKey')) {
    var selector = {}
    if (this.primaryKey.length == 1) {
      selector[this.primaryKey[0]] = val
//      var field = this.primaryKey[0]
//      t.fieldsNamesConvertion
//      if (this.state.needMongoIdConvertion &&
//          this.fields.hasOwnProperty(this.primaryKey[0]) && 
//          this.fields[this.primaryKey[0]].type == 'id') 
//        return { this.convert2MongoObjectID(val)
//      else return this.convert2MongoObjectID(val)
    } else {
      if (_.isString(val)) val = valuesLib.comma_str2array(val)
      if (!_.isArray(val))
        this.logError('Для множественного индекса Id должно быть строкой или массивом, ' +
                      ' а не ' + val)
      else {
        for (var i = 0; i < this.primaryKey.length; i++) {
          if (i >= val.length) {
            this.logError('Для множественного индекса Id недостаточно параметров: ' +
                      val)
            return val
          }
          selector[this.primaryKey[i]] = val[i]
        }
      }
    }
    if (!valuesLib.is_empty(selector))
      return this.convertFieldsForDatabase(selector)
  }
  return val
  
//  if (this.state.needMongoIdConvertion && this.primaryKey.length == 1 &&
//        this.fields.hasOwnProperty(this.primaryKey[0]) && 
//        this.fields[this.primaryKey[0]].type == 'id') 
//    return this.convert2MongoObjectID(val)
}

//MongoTab.prototype.convertMongoIdFields(obj) {
//}

MongoTab.prototype.convertFieldsForDatabase = function(obj) {
  if (!this.state.needMongoIdConvertion && !this.state.needFieldsNamesConvertion)
    return obj
  var obj = _.clone(obj)
  if (this.state.needMongoIdConvertion)
    for (var i = 0; i < this.mongoIdFields.length; i++)
      if (obj.hasOwnProperty(this.mongoIdFields[i]))
        obj[this.mongoIdFields[i]] =
          this.convert2MongoObjectID(obj[this.mongoIdFields[i]])
  if (this.state.needFieldsNamesConvertion)
    for (var i in this.fieldsNamesConvertion)
      if (this.fieldsNamesConvertion.hasOwnProperty(i) &&
         obj.hasOwnProperty(i)) {
        obj[this.fieldsNamesConvertion[i]] = obj[i]
        delete obj[i]
      }
  return obj
}

MongoTab.prototype.convertFieldsFromDatabase = function(obj) {
//cl('!this.state.needFieldsNamesConvertion', this.state.needFieldsNamesConvertion,
//   'fieldsNamesConvertion', this.fieldsNamesConvertion,
//  'obj', obj)
  if (this.state.needFieldsNamesConvertion) {
    _.each(this.fieldsNamesConvertion, function(value, key) {
//cl('!value, key', value, key)
      if (obj.hasOwnProperty(value)) {
        obj[key] = obj[value]
        delete obj[value]
      }
    })    
//cl('obj', obj)
  }
//  if (this.state.needMongoIdConvertion) {
//    for (var i in obj) if (obj.hasOwnProperty(i)) {
//      if (this.state.needMongoIdConvertion && 
//          (this.mongoIdFields.indexOf(i) > -1)) {
//        obj[i] = this.convert2MongoObjectID(obj[i])
//      }
//    }
//  }
  return obj
}



MongoTab.prototype.addConvertionFieldName = function(field, toConvert){
  if (!this.fieldsNamesConvertion) this.fieldsNamesConvertion = {}
  this.fieldsNamesConvertion[field] = toConvert
  this.state.needFieldsNamesConvertion = true
}

MongoTab.prototype.addIdFieldAndPrimaryKey = function() {
    this.primaryKey = ['id']
    if (!this.fields) this.fields = {}
    this.fields.id  = {type: 'id'}
    this.addConvertionFieldName('id', '_id')
    this.mongoIdFields = ['id']
    this.state.needMongoIdConvertion = true
}

MongoTab.prototype.checkFieldsAndPrimaryKey = function () {
  if (valuesLib.is_empty(this.fields)) {
    this.addIdFieldAndPrimaryKey()
    return
  }
  this.mongoIdFields = []
  var self = this
  _.each(this.fields, function(fld, key){
    if (fld.type === 'id') {
      self.mongoIdFields.push(key)
    }
  })
  if (valuesLib.is_empty(this.primaryKey)) {
//     if (this.mongoIdFields.length > 1 && !this.fields.id && !this.fields._id) {
//       this.setMulfunction('Указано больше 1-го поля c типом id и не указан primaryKey')
//       return
//     }
     if (this.fields._id) {
       this.primaryKey = ['_id']
     } else if (this.fields.id) {       
       this.primaryKey = ['id']
       this.addConvertionFieldName('id', '_id')
     } else if (this.mongoIdFields.length == 1) {
       this.primaryKey = [this.mongoIdFields[0]]
       this.addConvertionFieldName(this.mongoIdFields[0], '_id')
     } else this.addIdFieldAndPrimaryKey()
  }
  else {
    if (this.primaryKey.indexOf('id') > -1)
      this.addConvertionFieldName('id', '_id')
    else if (this.primaryKey.length == 1 && this.primaryKey[0] != '_id') 
      this.addConvertionFieldName(this.primaryKey[0], '_id')
    else if (this.primaryKey.length > 1 && this.mongoIdFields.length == 1)
      this.addConvertionFieldName(this.mongoIdFields[0], '_id')      
  }  
  
  if (this.mongoIdFields.length > 0) {
     this.state.needMongoIdConvertion = true
  }
}



//MongoTab.prototype.checkIdFieldAndPrimaryKey = function () {
//  var add_id = false
//  
//  this.id_convertion = true
//
//    // если отсутствует описание полей ИД будет добавлен
//  if (valuesLib.is_empty(this.fields)) {
//    this.fields = {}
//    add_id = true
//  }
//    // если есть стандартный монговский идентификатор
//  else if (this.fields._id) {
//    if (this.primaryKey || this.primaryKey[0] !== '_id')
//      this.log_warning('При указании в options.fields поля _id, ' + 
//        'недопустимо иное поле в options.primaryKey: '+ this.primaryKey)
//    this.id_convertion = false
//    this.primaryKey = ['_id']
//  }
//    // если не указан идентификатор в primaryKey
//  else if (!this.primaryKey || valuesLib.is_empty(this.primaryKey)) {
//    // если есть поле id оно будет считаться идентификатором, иначе ИД будет добавлен
//    if (this.fields.id) this.primaryKey = ['id']
//    else add_id = true
//  }
//  if (add_id) {
//    this.fields.id = {type: 'id'}
//    this.primaryKey = ['id']
//  }  
//}


//создает индекс для полей со свойством unique
MongoTab.prototype.checkOrCreateUnigueFieldIndex = function() {
  var fld, ind, keys, o
  for(var fld_name in this.fields) {
    fld = this.fields[fld_name] 
    if (fld.unique && (this.primaryKey.length > 1 ||
                       this.primaryKey[0] !== fld_name)) {
      ind = !!this.indexes && _.some(this.indexes, function(item){
        keys = _.keys(item.fields)
        return keys.length === 1 && keys[0] === fld_name && item.unique
      })
      if (!ind) {
        if (!this.indexes) this.indexes = {}
        o = {}
        o[fld_name] = 1
        o = {
          fields: o,
          unique: true
        }
        if (!fld.not_null) o.sparse = true
        this.indexes['automk_' + fld_name] = o
//        this.indexes.push(o)
      }
    }
  }
}

//
//MongoTab.prototype.special_mongo_types = function(type) {
//  var types = {id: true}
//  return !!types[type]
//}
//
//MongoTab.prototype.to_db_type = function (val, field, from_type) {
//  if (!field) return val
//  if (_.isString(field)) 
//    if (!this.fields[field]) return val
//    else field = this.fields[field]
//  if (this.special_mongo_types(field.type)) {
//    if (!from_type) from_type = valuesLib.value_type(val)
//    
//    if (field.type === 'id') {
//      if (from_type !== 'string') return val
//      else
//        try { return new mongodb.ObjectId(val) }
//        catch (e) { return val}
//    }
//  }
//  return DbTab.prototype.to_db_type.call(this, val, field, from_type)
//}
//
//
//MongoTab.prototype.id2mongo_id = function (obj, no_convert_values) {
//
//cl('this.primaryKey',this.primaryKey)
//  if (!this.primaryKey) return obj
//  else if ((this.primaryKey.length === 1) && (this.primaryKey[0] === '_id')) 
//    return obj
//  else {
//    if (this.primaryKey.length === 1) {
//      var f = this.primaryKey[0]
//      if (!obj[f]) return obj
//      else 
//        if (!no_convert_values && this.fields && this.fields[f]) 
//          obj._id = this.to_db_type(obj[f], this.fields[f])
//        else obj._id = obj[f]
//    }
//    else {
//      var o =  _.pick(obj, this.primaryKey)
//      if (valuesLib.is_empty(o)) return obj
//      else obj._id = _.values(o).join(KEY_FIELDS_DELIMITER)
//    }
//    for (var i in this.primaryKey)
//      delete obj[this.primaryKey[i]]
//    return obj
//  }
//}
//
//
//// переводит в вид id монго
//MongoTab.prototype.normalize_mongo_id = function (id) {  
//  if (this.no_normalize_id) return {_id: id}
//  if (this.primaryKey.length <= 1) {
//    if (_.isObject(id)) return id
//    else return {_id: this.to_db_type(id, this.primaryKey[0])}
//  }
//  else {
//    var obj, type = valuesLib.value_type(id)
//    if (type === 'string')
//      obj = _.object(this.primaryKey, valuesLib.comma_str2array(id))
//    else if (type === 'array') obj = _.object(this.primaryKey, id)
//    else if (type === 'object') obj = id
//    else return id
//    this.id2mongo_id(obj) 
//    return obj
//  }
//}
//
//MongoTab.prototype.mongo_id2id = function (obj) {
//  if (!this.primaryKey || 
//      (this.primaryKey.length === 1 && this.primaryKey[0] === '_id')) return
//  else {
//    if (this.primaryKey.length === 1) obj[this.primaryKey[0]] = obj._id
//    else {
//      var o = _.object(this.primaryKey, obj._id.split(KEY_FIELDS_DELIMITER))
//      this.normalize_values_types(o, true)
//      obj = _.extend(obj, o)
//    }
//    delete obj._id
//  }
//}
//
//// obj в виде {имя_поля: 'значение', ...}
//MongoTab.prototype.normalize_values_types = function (obj, from_string) {
//  var f, t
//  for (var i in obj) {
//    f = this.fields[i]
//    if (!f) continue
//    if ((from_string && f.type !== 'string') ||
//        (!from_string && f.type !== (t = this.db_value_type(obj[i])))) {
//      obj[i] = this.to_db_type(obj[i], f, t)
//    }
//  }    
//}


MongoTab.prototype.create = function (callback) { 
  var self = this;
  this.db.createCollection(this.name, this.write_concern, function(err, res) {
    if (self.wasError(err, 'create')) {
        if (callback) callback(err, null)
    }
    else {
      self.tab = res;
      self.check_db_structures(function(err, res) {
        if (callback) callback(err, self.tab)
      })
    }
//    if (self.indexes) self.create_indexes(callback)
//    else if (callback) callback(err, res);
  });
}


//MongoTab.prototype.index2createParams = function(name) {
//  var ind = this.indexes[name]
//  var o = {
//    spec: {
//      key: ind.fields
//    },
//    opt: {
//      name: name,
//    }
//  }
//  if (ind.unique) o.opt.unique = true
//  if (ind.sparse) o.opt.sparse = true
//  if (ind.geo) for (var i in o.spec.key) o.spec.key[i] = '2dsphere'
//  for (var j in this.write_concern) o.opt[j] = this.write_concern[j]
//  return o
//}

MongoTab.prototype.index2spec = function(name) {
  var ind = this.indexes[name]
  var o = {
    name: name,
    key: ind.fields
  }
  if (ind.unique) o.unique = true
  if (ind.sparse) o.sparse = true
  if (ind.geo) for (var i in o.key) o.key[i] = '2dsphere'
  for (var j in this.write_concern) o[j] = this.write_concern[j]
  return o
}


MongoTab.prototype.create_indexes = function (cb, bads) {
  var self = this
  if (!bads) bads = []
  if (!this.indexes || valuesLib.is_empty(this.indexes)) {
    if (cb) cb(null, true)
    return
  }
  var inds = []
  for (var i in this.indexes) 
    inds.push(this.index2spec(i))
    
  this.tab.createIndexes(inds, function (err, res) {
    var ok = false
    if (!self.wasError(err,'ошибка в create_indexes()')) {
      var q = res.numIndexesAfter - res.numIndexesBefore
      if (q === inds.length) {
        self.log_info('cоздано индексов - ' + inds.length)
        ok = true
      }
      else self.logError('не создались индексы - ' + (inds.length - q) + 
                        ' из ' + inds.length)        
    }
    if (cb) cb(err, ok)
  })
//  })
}

MongoTab.prototype.create_index = function (name, cb){
  var self = this
  if (!this.indexes || !this.indexes[name]) {
    this.logError('для создания индекса '+ name + ' отсутствуют параметры')
    cb(null, false)
  }
  else {
//    var ind = this.indexes[name]
    var spec = this.index2spec(name)
    this.tab.createIndex(spec.key, _.extend({},_.omit(spec, 'key'), this.write_concern), 
                function(err, res){
      if (self.wasError(err, 'create_index(): ' + name)) callback(err, false)
      else if (res !== spec.name) {
        self.logError('create_index(): какие-то проблемы при создании индекса - ' + name)
        cb(null, false)
      }
      else {
        self.log_info('создан индекс - ' + name)
        cb(null, true)
      }
    })
  }
}

//> RESULT>
// { nIndexesWas: 3, ok: 1 }
MongoTab.prototype.drop_index = function (index_name, cb){
  var self = this
  this.tab.dropIndex(index_name, this.write_concern, function (err, res) {
    var ok = false
    if (!self.wasError(err, 'drop_index(): ' + index_name))
      if (res.ok !== 1) 
        self.logError('drop_index(): какие-то проблемы при удалении - ' + index_name)
      else {
        self.log_info('удален индекс - ' + index_name)
        ok = true
      }
    if (cb) cb(err, ok)
  })
}


MongoTab.prototype.drop_indexes = function (cb){
  var self = this
  this.tab.dropAllIndexes(function (err, res) {
    var ok = false
    if (!self.wasError(err, 'drop_indexes(): ошибка при удалении индексов'))
      if (res !== true) 
        self.logError('drop_indexes(): какие-то проблемы при удалении - ' + res)
      else {
        self.log_info('удалены дополнительные индексы')
        ok = true
      }
    if (cb) cb(err, ok)
  })
}


// возвращает true в случае удаления
MongoTab.prototype.drop = function (callback) { 
  var self = this;
  this.tab.drop(function(err, res) {
//    self.wasError(err, 'drop(): ошибка при удалении коллекции')
    callback(err, res);
  });
}

// добавляет документ
MongoTab.prototype.add_func = function (obj, callback, options) {
  var self = this

  if (!options.no_change_id && this.state.needMongoIdConvertion) {
    for (var i = 0; i < this.mongoIdFields.length; i++) {
      if (!obj.hasOwnProperty(this.mongoIdFields[i])) 
        obj[i] = new mongodb.ObjectId()
    }
    
    
//    for (var i in this.fields)
//      if (this.fields[i].type === 'id')
//        obj[i] = new mongodb.ObjectId()
  }
  
//  if (this.id_convertion) this.id2mongo_id(obj)
  
  this.tab.insertOne(this.convertFieldsForDatabase(obj),
    this.write_concern, function(e, d){
//cl('in deep', d)
    if (e) callback(e, d);
    else {
//      var o = d.ops[0];
//      self.convertFieldsFromDatabase(o)
//      if (self.id_convertion) self.mongo_id2id(o)
      callback(null, self.convertFieldsFromDatabase(d.ops[0]));
    }
  });
}

// cb - function(err, num)
MongoTab.prototype.getNextId = function(fld, cb) {
  if (!cb) {
    cb = fld
    if (this.primaryKey && this.primaryKey.length == 1)
      fld = this.primaryKey[0]
  }
  if (!this.fields.hasOwnProperty(fld) || !this.fields[fld].auto_inc)
    cb(null, null)
  else {
    var self = this
    this.store.getNextSequence(this.make_sequence_name(fld), function(err, num){
      if (err) cb(err, null)
//      else if (self.fields[fld].auto_rnd_inc)
//        cb(null, self.genRndIdByIncId(fld, num))
      else cb(null, num)    
    })
  }
}

//MongoTab.prototype.getNextId = function(fld, cb) {
//  if (!this.fields.hasOwnProperty(fld) || 
//      (!this.fields[fld].auto_inc && !this.fields[fld].auto_rnd_inc))
//    cb(null, null)
//  else {
//    var self = this
//    this.store.getNextSequence(this.make_sequence_name(fld), function(err, num){
//      if (err) cb(err, null)
//      else if (self.fields[fld].auto_rnd_inc)
//        cb(null, self.genRndIdByIncId(fld, num))
//      else cb(null, num)    
//    })
//  }
//}


//MongoTab.prototype.add = function (obj, callback, no_change_id) {
//  var id_fld = this.fields._id
//  
//  
////  propsSetted 
//  
//  if (this.id_convertion && obj.id) this.id2mondo_id(obj)
//  if (!no_change_id) {
//    if (id_fld.type = 'id') obj._id = new mongodb.ObjectId()
//    else if (id_fld.auto_inc) 
//      obj._id = this.db.getNextSequence(this.name + '__id');
//  }
//  for (var i in this.fields)
//    if (this.fields[i].auto_inc && i !== '_id') 
//      obj[i] = this.db.getNextSequence(this.name + '_' + i);
//  this.tab.insertOne(obj, function(e, d){
//    if (e) callback(e, d);
//    else {
//      var o = d.ops[0];
//      if (this.id_convertion) this.mongo_id2id(o)
//      callback(null, o);
//    }
//  });
//}
//opts = ['string,object', ,'function']
// 1й параметр 
// MongoTab.prototype.check_args = function (args, opts) {
//  _.keys(a)
//}

/* 
  возвращает запись по ид в колбэке, если не найдено возвращает null
  get = function (id, callback)
  не реализовано get = function (id, [fields,] callback)
  не реализовано get = function ({field: name, value: value}, callback)
*/
MongoTab.prototype.get = function (id, callback) {
  var self = this
//  if (!callback) { 
//    callback = fields
//    fields = undefined 
//  } 
//  if (!valuesLib.is_object) 
//  if (this.id_convertion) this.id2mongo_id(obj)
//  
//  if (this.fields._id.type === 'id' && !(id instanceof mongodb.ObjectID))
//    id = new mongodb.ObjectID(id);
//cl('get0',id ,callback)
//  if (!_.isObject(id))
  
//  this.tab.findOne(this.normalizeIdField(id), function(e, d) {
  this.tab.findOne(this.makeIdSelector(id), function(e, d) {
//cl('get',e,d)
    if (self.wasError(e, 'get')) callback(e);
    else if (d !== null) {
      if (callback) {
        self.convertFieldsFromDatabase(d)
//      if (self.id_convertion) self.mongo_id2id(d)
        callback(null, d);
      }
    }
    else if (callback) callback(null, d);
  });
}

/* 
  сохраняет запись по ид, изменяет частично (только указанные поля)
  возвращает true или false
*/
MongoTab.prototype.update = function (id, obj, callback) {
  var self = this
  
//cl(id,obj,callback)
//  if (this.fields._id.type === 'id' && !(id instanceof mongodb.ObjectID))
//    id = new mongodb.ObjectID(id);
//  delete obj.id;
//  delete obj._id;
//  console.log(id instanceof mongodb.ObjectID, id, obj);
//  this.tab.updateOne(this.normalize_mongo_id(id), {$set: obj}, this.write_concern,
//                     function(e, r){
  this.tab.updateOne(this.makeIdSelector(id), {$set: this.convertFieldsForDatabase(obj)},
                     this.write_concern, function(e, r){
    if (self.wasError(e, 'update')) callback(e, false);
    else if (r.matchedCount === 1 && r.modifiedCount === 1)
      callback(null, true);
    else callback(null, false); 
  });
}

/* 
  сохраняет запись по ид, заменяет документ
  возвращает true или false
*/
MongoTab.prototype.set_func = function (id, obj, callback) {
  var self = this
//  if (this.fields._id.type === 'id' && !(id instanceof mongodb.ObjectID))
//    id = new mongodb.ObjectID(id);
//  delete obj.id;
//  delete obj._id;
//  if (this.id_convertion) this.id2mongo_id(obj)
//cl('set_func1', id, obj)
//  var sel = this.makeIdSelector(id)
//  var dat = this.convertFieldsForDatabase(obj)
//cl('set_func2', sel, dat)
//  this.tab.updateOne(sel, dat, function(e, r){
    
  this.tab.updateOne(this.makeIdSelector(id), 
    this.convertFieldsForDatabase(obj), function(e, r){
    
//this.tab.updateOne(this.normalize_mongo_id(id), obj, function(e, r){
//cl('set',e,r)
    if (self.wasError(e, 'set')) callback(e, false)
    else if (r.matchedCount === 1 && r.modifiedCount === 1)
      callback(null, true);
    else callback(null, false); 
  });
}


// возвращает количество удаленных записей
MongoTab.prototype.remove = function (id, callback) {
  var self = this
//  if (this.fields._id.type === 'id' && !_.isObject(id) && 
//      !(id instanceof mongodb.ObjectID)) 
//    id = new mongodb.ObjectID(id);
//  this.tab.remove(this.normalize_mongo_id(id), function(e, r) {
  this.tab.remove(this.makeIdSelector(id), function(e, r) {
    if (self.wasError(e, 'remove')) callback(e, 0);
    else if (r.result && r.result.ok === 1) callback(null, r.result.n);
    else {
      e = new Error('Непонятная ошибка при удалении')
      self.wasError(e, 'remove')
      callback(e, 0);
    }
  });
}


/**
* количество записей
* @function
* @param (Object) [options] условия
* @param (Object) [options.not_consider_skip_limit] не брать в расчет условия skip, limit
* @param (function) callback аргументы (err, result)
*/
MongoTab.prototype.count = function (options, callback) {
  if (!callback) {
    callback = options
    options = {}
  }
  if (this.isNoQueryConditions()) this.tab.count(callback)
  else {
    var opts
    this.makeQueryObjects()
    if (options.not_consider_skip_limit)
      opts = _.omit(this.queryOptions, 'skip', 'limit') 
    else opts = this.queryOptions
    this.tab.count(this.querySelector, opts, callback)
  }
}

// callback(error, boolean)
MongoTab.prototype.exist = function (callback) {
  this.db.admin().validateCollection(this.name, function (e, r) {
//console.log('in exist', e, r);
//console.log('in exist e.message', e.message);
    if (e) {
      if (e.message == 'ns not found') callback(null, false); 
      else callback(e, false)
    }
    else callback(null, true);
  });
}

// options {force: true} перезаписать если имеется
MongoTab.prototype.rename = function (newName, options, callback) {
  if (callback === undefined) {
    callback = options
    options = undefined
  }
  var callOptions = {}
  var self = this
  if (options && options.force) callOptions = {dropTarget: true}
  this.tab.rename(newName, callOptions, function (e, r) {
//cl('in hoper', e, r)
    if (e) {
      if (callback) callback(e, false)
    } else {
//cl('IN HOPER2')
      if (self.hasOwnProperty('store') && self.store.hasOwnProperty('tabs')) {
        self.store.tabs[newName] = self
        delete self.store.tabs[self.name]
      }
      self.tab = self.db.collection(newName)
//cl('self.name, newName', self.name, newName)
      self.name = newName
//cl('self.name, newName', self.name, newName)
      if (callback) callback(null, true)
    }
  });
}
















