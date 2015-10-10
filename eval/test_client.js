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
    ConsistentHashing = require('consistent-hashing'),
    constants = require('./constants'),
    ioServer = new Server();

var argv = require('optimist')
    .usage('Usage: $0 -c [CONFIG] -p [PORT] -k [KEY_RANGE]')
    .demand(['c', 'p'])
    .alias('c', 'config')
    .describe('c', 'Config file with list of ip and port pair identifying peers')
    .alias('p', 'port')
    .describe('p', 'Port to run peer on')
    .alias('k', 'keyRange')
    .describe('k', 'Key Range')
    .argv;

var peers = validateConfig(argv.config),
    peersList = new ConsistentHashing(peers);

// NOTE: Validate config file
if (!peers) {
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
        choices: [constants.TEST_PUT, constants.TEST_GET, constants.TEST_DELETE]
    }];

    inquirer.prompt(requestForOperation, function( response ) {
        doTest(response.operation);
    });
}

function doTest(operation) {
    switch (operation) {
        case constants.TEST_PUT:
            testPut();
            break;
        case constants.TEST_GET:
            testGet();
            break;
        case constants.TEST_DELETE:
            testDelete();
            break;
        default:
            logServerMessage("ERROR: SOMETHING WENT TERRIBLY WRONG !");
    }
}

var iteration = 0;
var totalLatency = 0;
var keyRange = argv.keyRange;
var maxIteration = 100;

function testPut() {
    if (iteration < maxIteration) {
        delegateOperationToPeer(keyRange.toString(), constants.TEST_PUT, { key: keyRange.toString(), value: keyRange.toString() + '_value' });
        keyRange++; iteration++;
    } else {
        console.log("Total Put Latency / Lookup (ms) : ", totalLatency / maxIteration);

        iteration = 0;
        totalLatency = 0;
        keyRange = argv.keyRange;

        listOperations();
    }
}

function testGet() {
    if (iteration < maxIteration) {
        delegateOperationToPeer(keyRange.toString(), constants.TEST_GET, { key: keyRange.toString(), value: keyRange.toString() + '_value' });
        keyRange++; iteration++;
    } else {
        console.log("Total Get Latency / Lookup (ms) : ", totalLatency / maxIteration);

        iteration = 0;
        totalLatency = 0;
        keyRange = argv.keyRange;

        listOperations();
    }
}

function testDelete() {
    if (iteration < maxIteration) {
        delegateOperationToPeer(keyRange.toString(), constants.TEST_DELETE, { key: keyRange.toString(), value: keyRange.toString() + '_value' });
        keyRange++; iteration++;
    } else {
        console.log("Put Latency / Lookup (ms) : ", totalLatency / maxIteration);

        iteration = 0;
        totalLatency = 0;
        keyRange = argv.keyRange;

        listOperations();
    }
}

// NOTE: perform specific operation based on 'operation' using the 'key' and 'value'
function performOperation(operation, key, value) {
    var status = constants.NOOP;

    switch (operation) {
        case constants.TEST_PUT:
            status = putValue(key, value);
            break;
        case constants.TEST_GET:
            status = getValue(key);
            break;
        case constants.TEST_DELETE:
            status = deleteKey(key);
            break;
        default:
            logServerMessage("ERROR: SOMETHING WENT TERRIBLY WRONG !");
    }

    logServerMessage(operation + " : Status => " + status);

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

// NOTE: Find target peer using ConsistentHashing
function findTargetPeer(key) {
    console.log("Key : " + key + ", TypeOf : " + typeof key);
    return peersList.getNode(key);
}

function delegateOperationToPeer(key, operation, operation_params) {
    var socket_address;
    var peerID = findTargetPeer(key);

    if (validateAddress(peerID)) {
        socket_address = "http://" + peerID.split(" ").join(":");
    } else {
        logClientMessage("ERROR : SOMETHING TERRIBLY WENT WRONG WHILE CONNECTING TO PEER !");
        process.exit();
    }

    console.log("Connecting to peer : ", socket_address);
    var socket = io(socket_address, { 'forceNew': true });

    socket.on('op_status', function (response) {
       logClientMessage(operation + " : Status => " + response.status);

        var latency = Date.now() - response.timestamp;
        console.log(operation + " Latency : ", latency);
        totalLatency += latency;

        doTest(operation);
    });

    socket.on('connect', function () {
        logClientMessage("Connected to Peer Server !");
        socket.emit('operation', { operation: operation, params: operation_params, timestamp: Date.now() });
    });
}

// NOTE: DHT Peer Server
ioServer.on('connect', function (socket) {
    logServerMessage("Connected with Peer Client : " + socket.handshake.address);

    socket.on('operation', function (response) {
        var status = performOperation(response.operation, response.params.key, response.params.value);
        socket.emit('op_status', { status:  status, timestamp: response.timestamp });
    });
});

// NOTE: validate the config file for correct peer addresses
function validateConfig(fileName) {
    var peers = fs.readFileSync(fileName).toString().split('\n');

    var invalidPeers = peers.filter(function (peer, i) {
        return !validateAddress(peer);
    });

    return invalidPeers.length > 0 ? false : peers;
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
console.log("\n Server running at : " + ip.address() + ":" + argv.port);
