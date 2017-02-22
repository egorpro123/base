var _ = require('underscore')
var config = require('../../config.json');
var cl = require('../../utils/console_log').server_log;

config.db || (config.db = {});
_.defaults(config.db, {
  type: "mongodb",
  host: "localhost",
  port: "4000",
  name: "main",
  connection_pool: 100
}); 
module.exports = config
