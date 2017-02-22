var _ = require('underscore');
var dummy = require('./dummy')
var valuesLib = require('../../utils/values')

/*
  таблица - заготовка для последующей реализации,
    имеется реализация запоминания условий для запросов
  параметры передаваемые в options:
    name - наименование
*/

var Tab = exports.Tab = function (options) {
  dummy.Dummy.call(this, options)
  this.clearQueryRules();
}

valuesLib.inherit(Tab, dummy.Dummy);

Tab.prototype.caption = 'Таблица'

Tab.prototype.type = 'dummy_tab'

var tabMainMethods = exports.tabMainMethods = 
  ['add', 'get', 'update', 'set', 'remove', 'count', 'one', 'all']
dummy.setMethodsUndefined(Tab.prototype, tabMainMethods)

/* 
  формирует выборку по условиям в виде:
  .query().qt('sum', 100).lt('sum', 1000).or().eq('name','greg',{case: true}).one()
  или all()
  or() действует только на первом уровне (т.е. только ()or(), например, такое выражение не 
    работает (()or())or()
  eq('name','greg',{case: true}) сравнения, case с учетом регистра (по умолчанию false)
  
  др.условия in(field_name, [val1,val2,...]) совпадает с любым из значений
    notin (field_name, [val1,val2,...]) не одно из значений
    all(field_name, [val1,val2,...]) соотв.всем указаным значениям
    lte(field_name, value) меньше или равно
    qte(field_name, value) больше или равно
    like(field_name, fragment) совпадение фрагмента строки
*/
Tab.prototype.query = function () {
  this.clearQueryRules();
  return this;
}


Tab.prototype.clearQueryRules = function () {
  this.rules = [[]];
  this.current_rule = 0;
}


Tab.prototype.isNoQueryConditions = function () {
//cl('in isNoQueryConditions',_.isEqual(this.rules, [[]]), this.rules)
  return _.isEqual(this.rules, [[]]);
}


Tab.prototype.or = function () {
  this.current_rule++;
  return this;
}


var funcs = ['eq', 'qt', 'qte', 'lt', 'lte', 'like', 'ne'];
_.each(funcs, function(func) {
  Tab.prototype[func] = function(fld_name, value, options) {
    fld_name = ("" + fld_name).trim()
    this.rules[this.current_rule].push({
      type: func,
      fld: fld_name,
      val: value,
      options: options
    });
    return this;
  };
});


var funcs = ['in', 'notin', 'inall'];
_.each(funcs, function(func) {
  Tab.prototype[func] = function(fld_name, value, options) {
    fld_name = ("" + fld_name).trim()
    var o = {
      type: func,
      fld: fld_name,
      options: options
    }
    if (_.isArray(value)) o.val = value;
    else o.val = [value];
    this.rules[this.current_rule].push(o);
    return this;
  };
});



