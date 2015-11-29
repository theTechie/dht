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
    ConsistentHashing = require('consistent-hashing'),
    constants = require('./constants'),
    KeyProvider = require('./keyProvider');

var log = true;

var argv = require('optimist')
    .usage('Usage: $0 -c [CONFIG] -k [KEY_RANGE]')
    .demand(['c', 'k', 'i'])
    .alias('c', 'config')
    .describe('c', 'Config file with list of ip and port pair identifying peers')
    .alias('k', 'keyRange')
    .describe('k', 'Key Range')
    .alias('i', 'iterations')
    .describe('i', 'Num of Iterations')
    .argv;

var peers = validateConfig(argv.config),
    peersList = new ConsistentHashing(peers);

var iteration = 0;
var totalLatency = 0;
var maxIteration = argv.iterations;
var sockets = new HashTable();

// NOTE: Validate config file
if (!peers) {
    console.log("Please enter valid IP address and port separated by a space in config file ! : [IP_ADDRESS] [PORT] => ", argv.config);
    process.exit();
}

// NOTE: Prepare Keys
KeyProvider.init(argv.keyRange, argv.iterations);

doTest(constants.PUT);

function doTest(operation) {
    switch (operation) {
        case constants.PUT:
            testPut();
            break;
        case constants.GET:
            testGet();
            break;
        case constants.DELETE:
            testDelete();
            break;
        default:
            logClientMessage("ERROR: SOMETHING WENT TERRIBLY WRONG !");
    }
}

function testPut() {
    if (iteration < maxIteration) {
        delegateOperationToPeer(KeyProvider.getKey(iteration), constants.PUT, { key: KeyProvider.getKey(iteration), value: KeyProvider.getValue(iteration)});
        iteration++;
    } else {
        console.log("Total Put Latency / Lookup (ms) : ", totalLatency / maxIteration);
        iteration = 0;
        totalLatency = 0;

        doTest(constants.GET);
    }
}

function testGet() {
    if (iteration < maxIteration) {
        delegateOperationToPeer(KeyProvider.getKey(iteration), constants.GET, { key: KeyProvider.getKey(iteration), value: KeyProvider.getValue(iteration)});
        iteration++;
    } else {
        console.log("Total Get Latency / Lookup (ms) : ", totalLatency / maxIteration);

        iteration = 0;
        totalLatency = 0;

        doTest(constants.DELETE);
    }
}

function testDelete() {
    if (iteration < maxIteration) {
        delegateOperationToPeer(KeyProvider.getKey(iteration), constants.DELETE, { key: KeyProvider.getKey(iteration), value: KeyProvider.getValue(iteration)});
        iteration++;
    } else {
        console.log("Total Delete Latency / Lookup (ms) : ", totalLatency / maxIteration);

        iteration = 0;
        totalLatency = 0;

        process.exit();
    }
}

// NOTE: Find target peer using ConsistentHashing
function findTargetPeer(key) {
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

    var socket;

    if (sockets.has(peerID)) {
        socket = sockets.get(peerID);
        socket.emit('operation', { operation: operation, params: operation_params, timestamp: Date.now() });
    } else {
        socket = io(socket_address);

        console.log("Connecting to peer : ", socket_address);

        socket.on('op_status', function (response) {
            logClientMessage(response.operation + " : Status => " + response.status);

            var latency = Date.now() - response.timestamp;
            //console.log(response.operation + " Latency : ", latency);
            totalLatency += latency;

            doTest(response.operation);
        });

        socket.on('connect', function () {
            logClientMessage("Connected to Peer Server !");
            socket.emit('operation', { operation: operation, params: operation_params, timestamp: Date.now() });
        });

        sockets.put(peerID, socket);
    }
}

// NOTE: validate the config file for correct peer addresses
function validateConfig(fileName) {
    var peers = fs.readFileSync(fileName).toString().split('\n');

    peers = peers.filter(function (peer, i){
        return peer.length > 0;
    });
    
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
    if (log)
        console.log("[Client] : ", message);
}
