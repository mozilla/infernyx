This project contains data processing rules for the Disco and Inferno mapreduce rules for the Mozilla Tiles project.dd

The payload it expects is:

    {
      "timestamp": 1407336655489, # unix timestamp, injected by onyx
      "date": "2014-05-27", # iso formatted date string for easy splitting by date, injected by onyx
      "ip": "103.242.154.10", # request originator's IPv4 address, injected by onyx
      "ua": "Mozilla/5.0 (Windows NT 5.1; rv:33.0) Gecko/20100101 Firefox/33.0", # request originator's UA, injected by onyx
      "locale": "en-US", # locale str, sent by client
      "hll_index": 2754, # hyperloglog index parameter, sent by client
      "hll_value": 8, # hyperloglog index parameter, sent by client
      "click": 2, # this denotes a click action, and the index of the tile click in the "tiles" array. other possible actions: "block", "pin", "unpin"
      "tiles": [
        {
          "url": "http://stackoverflow.com/questions/5998245/get-current-time-in-milliseconds-in-python", # url if a history tile, absent otherwise
          "id": 8, # tile id if it is an enhanced tile or directory tile. absent otherwise
          "pin": true # if the tile is pinned, absent otherwise
        },
        ...
      ]
    }

valid requests from firefox, one per line:

    {"locale":"en-US","tiles":[{"id":1},{"id":2},{"id":3},{"id":4},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}]} # impression ping, because there are no actions
    {"locale":"en-US","tiles":[{"id":1,"pin":1},{"id":2},{"id":3},{"id":4},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}],"pin":0} # click ping, the action being "pin"
    {"locale":"en-US","tiles":[{"id":1},{"id":2},{"id":3},{"id":4},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}],"unpin":0} # click ping, the action being "unpin"
    {"locale":"en-US","tiles":[{"id":1},{"id":2},{"id":3},{"id":4},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}],"block":0} # click ping, the action being "block"
    {"locale":"en-US","tiles":[{"id":2},{"id":3},{"id":4},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}]} # impression ping, after the "block" action
    {"locale":"en-US","tiles":[{"id":5,"url":"https://www.mozilla.org/en-US/about/"},{"id":2},{"id":3},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}]} # impression ping, with an enhanced history tile
    {"locale":"en-US","tiles":[{"id":5,"url":"https://www.mozilla.org/en-US/about/"},{"url":"https://twitter.com/Firefox"},{"id":2},{"id":3},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}]} # impression ping, with an enhanced and a regular history tile
    {"locale":"en-US","tiles":[{"id":5,"url":"https://www.mozilla.org/en-US/about/"},{"id":2},{"id":3},{"id":6},{"id":8},{"id":9},{"id":10},{"pin":1,"pos":8,"url":"https://twitter.com/Firefox"}]} # note the empty tile at position 7

the web server listening to requests at the api endpoints will then inject a couple of parameters in the log

these include:
 * timestamp: a unix timestamp at time of receipt, in UTC
 * date: an iso formatted date string for easy splitting by date, in UTC
 * ip: request originator's IPv4 address
 * ua: request originator's UA
