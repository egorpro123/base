var log = require('winston');
var _ = require('underscore');
var config = require('./db_config')

var cl = require('../../utils/console_log').server_log

/*
  класс - заготовка для последующей реализации
  параметры передаваемые в options:
    name - наименование
*/


exports.setMethods = function (obj, method_names, method) {
  _.each(method_names, function(func) {
    obj[func] = _.bind(method, obj, func)
  })
}

exports.setMethodsUndefined = function (obj, method_names) {
  this.setMethods(obj, method_names,
  function (func_name) {
    this.logError('вызвана не реализованная функция ' + func_name)
  })
}


var Dummy = exports.Dummy = function (options) {
  
  options || (options = {})
  if (!options.name || options.name === '')
    this.logError('при создании не указано имя в options.name');
  else if (!_.isString(options.name)) 
    this.logError('имя д.б.строкой, а не - ' + options.name)
  else this.name = options.name
  this.errors_counter = 0
  // отключение логирования для тестов
//  if (options.dont_log) {
//    this.logError = function (msg) {
//      if (_.isArray(msg)) this.errors_counter += msg.length
//      else this.errors_counter++
//    }
//    this.log_info = this.log_warning = function () {}
//  }
}

Dummy.prototype.type = 'Dummy'

Dummy.prototype.caption = 'Класс-шаблон'

Dummy.prototype.log_ticket = function () {
  return this.caption + ' ' + this.name + ' (' + this.type + '): '
}

// prefix - необязательное добавление
Dummy.prototype.logError = function (msg, prefix) {
  if (this.errors_counter > 10000000) this.errors_counter = 0
  if (!prefix) prefix = ''
  else prefix += ' '
  if (_.isArray(msg)) {
    for (var i in msg) log.error(this.log_ticket() + prefix + msg[i])
    this.errors_counter += +i + 1
  }
  else {
    log.error(this.log_ticket() + prefix + msg)
    this.errors_counter++
  }
}

Dummy.prototype.log_info = function (msg) {
  if (_.isArray(msg)) 
    for (var i in msg) log.info(this.log_ticket() + msg[i])
  else log.info(this.log_ticket() + msg)
}

Dummy.prototype.log_warning = function (msg) {
  if (_.isArray(msg)) 
    for (var i in msg) log.warn(this.log_ticket() + msg[i])
  else log.warn(this.log_ticket() + msg)
}

Dummy.prototype.wasError = function (err, caller_func_name) {
  if (err) {
    this.logError(caller_func_name + ': ' + err.message)
    return true
  }
}

Dummy.prototype.get_param = function (options, param_name) {
  if (options[param_name]) return options[param_name]
  else if (config.db[param_name]) return config.db[param_name]
}

