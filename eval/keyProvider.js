var KVP = [],
    keyLength = 10,
    valueLength = 90;

function init(startRange, numOfKeys) {
  var endRange = startRange + numOfKeys;
  
  for (var i = startRange; i < endRange; i++) {
    KVP.push({
      key : i + makeString(keyLength),
      value: makeString(valueLength)
    });
  }
}

// NOTE: will return unique KVP randomly
function getKey(index) {
  return KVP[index].key;
}

function getValue(index) {
  return KVP[index].value;
}

function makeString(length) {
    var text = "";
    var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    for( var i = 0; i < length; i++ )
        text += possible.charAt(Math.floor(Math.random() * possible.length));

    return text;
}

module.exports = {
  init: init,
  getKey: getKey,
  getValue: getValue
};