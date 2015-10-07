/**
 * Created by gags on 4/10/15.
 */
var inquirer = require('inquirer'),
    io = require('socket.io-client'),
    fs = require('fs'),
    path = require('path'),
    ip = require('ip'),
    ss = require('socket.io-stream'),
    Server = require('socket.io'),
    HashTable = require('hashtable'),
    hashtable = new HashTable(),
    constants = require('./constants'),
    ioServer = new Server();

var argv = require('optimist')
    .usage('Usage: $0 -c [CONFIG] -p [PORT]')
    .demand(['c', 'p'])
    .alias('c', 'config')
    .describe('c', 'Config file with list of ip and port pair identifying peers')
    .alias('p', 'port')
    .describe('p', 'Port to run peer on')
    .argv;

// NOTE: Validate config file
if (!validateConfig(argv.config)) {
    console.log("Please enter valid IP address and port separated by a space in config file ! : [IP_ADDRESS] [PORT] => ", argv.config);
    process.exit();
}

listOperations();

// NOTE: List the operations supported by DHT
function listOperations() {
    var requestForOperation = [{
        type: "list",
        name: "operation",
        message: "Please select the operation you would like to perform : ",
        choices: ['PUT', 'GET', 'DELETE']
    }];

    inquirer.prompt(requestForOperation, function( response ) {
        requestKey(response.operation);
    });
}

// NOTE: request for 'key' to be used
function requestKey(operation) {
    var requestForKey = [{
        type: "input",
        name: "key",
        message: "Please enter the key : ",
        validate: function (input) {
            if (input === undefined || input === '' || input.trim() === '') {
                console.log("Please enter proper key !");
                return false;
            } else {
                return true;
            }
        }
    }];

    inquirer.prompt(requestForKey, function( response ) {
        // NOTE: Only PUT operation requires 'value'; GET and DELETE requires only the 'key'
        if (operation === constants.PUT) {
            requestValue(operation, response.key);
        } else {
            performOperation(operation, response.key);
            listOperations();
        }
    });
}

// NOTE: request for 'value' to be stored (for PUT)
function requestValue(operation, key) {
    var requestForValue = [{
        type: "input",
        name: "value",
        message: "Please enter the value : ",
        validate: function (input) {
            if (input === undefined || input === '' || input.trim() === '') {
                console.log("Please enter proper value !");
                return false;
            } else {
                return true;
            }
        }
    }];

    inquirer.prompt(requestForValue, function( response ) {
        performOperation(operation, key, response.value);
        listOperations();
    });
}

// NOTE: perform specific operation based on 'operation' using the 'key' and 'value'
function performOperation(operation, key, value) {
    var status = constants.NOOP;

    switch (operation) {
        case constants.PUT:
            status = putValue(key, value);
            break;
        case constants.GET:
            status = getValue(key);
            break;
        case constants.DELETE:
            status = deleteKey(key);
            break;
        default:
            logServerMessage("ERROR: SOMETHING WENT TERRIBLY WRONG !");
    }

    logServerMessage(" : " + operation + " : Status => " + status);

    return status;
}

function putValue(key, value) {
    var status = hashtable.put(key, value);
    return status ? true : false;
}

function getValue(key) {
    var value = hashtable.get(key);
    return value ? value : null;
}

function deleteKey(key) {
    return hashtable.remove(key);
}

function hash(key) {
    // TODO: perform hashing and return the peerID to connect to
}

function delegateOperationToPeer(peerID, operation, operation_params) {
    var socket_address;

    if (validateAddress(peerID)) {
        socket_address = "http://" + peerID;
    } else {
        logClientMessage("ERROR : SOMETHING TERRIBLY WENT WRONG WHILE CONNECTING TO PEER !");
        process.exit();
    }

    var socket = io(socket_address);

    socket.on('op_status', function (response) {
       logClientMessage(" : " + operation + " : Status => " + response.status);
    });

    socket.on('connect', function () {
        logClientMessage("Connected to Peer Server !");
        socket.emit('operation', { operation: operation, params: operation_params });
    });
}

// NOTE: DHT Peer Server
ioServer.on('connect', function (socket) {
    logServerMessage("Connected with Peer Client : " + socket.handshake.address);

    socket.on('operation', function (response) {
        var status = performOperation(response.operation, response.params.key, response.params.value);
        socket.emit('op_status', { status:  status });
    });
});

// NOTE: validate the config file for correct peer addresses
function validateConfig(fileName) {
    var peers = fs.readFileSync(fileName).toString().split('\n');

    var invalidPeers = peers.filter(function (peer, i) {
        return !validateAddress(peer);
    });

    return invalidPeers.length > 0 ? false : true;
}

// NOTE: check if address is valid (ip:port)
function validateAddress(entry) {
    var ip_port = entry.split(" ");
    var blocks = ip_port[0].split(".");

    if (ip_port.length < 2)
        return false;

    if(blocks.length === 4) {
        return blocks.every(function(block) {
            return parseInt(block,10) >=0 && parseInt(block,10) <= 255;
        });
    }
    return false;
}

// NOTE: log client message
function logClientMessage(message) {
    console.log("[Client] : ", message);
}

// NOTE: log server message
function logServerMessage(message) {
    console.log("[Server] : ", message);
}

ioServer.listen(argv.port);
