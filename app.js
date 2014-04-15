var helpers = require("./helpers");
var AWS = require("aws-sdk");

var AWS_CONFIG_FILE = "./config.json";


AWS.config.loadFromPath(AWS_CONFIG_FILE);

