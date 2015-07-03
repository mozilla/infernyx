This project contains data processing rules for the Disco and Inferno mapreduce rules for the Mozilla Tiles project.dd

Infernix Input Payloads
-----------------------

The payload it expects is:

    {
      "timestamp": 1407336655489, # unix timestamp, injected by onyx
      "date": "2014-05-27", # iso formatted date string for easy splitting by date, injected by onyx
      "ip": "103.242.154.10", # request originator's IPv4 address, injected by onyx
      "ua": "Mozilla/5.0 (Windows NT 5.1; rv:33.0) Gecko/20100101 Firefox/33.0", # request originator's UA, injected by onyx
      "locale": "en-US", # locale str, sent by client
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

Payloads from Firefox
---------------------

Valid request examples from firefox, one per line:

Impression ping, because there are no actions

    {"locale":"en-US","tiles":[{"id":1},{"id":2},{"id":3},{"id":4},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}]}

Click ping, the action being "pin"

    {"locale":"en-US","tiles":[{"id":1,"pin":1},{"id":2},{"id":3},{"id":4},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}],"pin":0}

Click ping, the action being "unpin"

    {"locale":"en-US","tiles":[{"id":1},{"id":2},{"id":3},{"id":4},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}],"unpin":0}

Click ping, the action being "block"

    {"locale":"en-US","tiles":[{"id":1},{"id":2},{"id":3},{"id":4},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}],"block":0}

Impression ping, after the "block" action

    {"locale":"en-US","tiles":[{"id":2},{"id":3},{"id":4},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}]}

Impression ping, with an enhanced history tile

    {"locale":"en-US","tiles":[{"id":5,"url":"https://www.mozilla.org/en-US/about/"},{"id":2},{"id":3},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}]}

Impression ping, with an enhanced and a regular history tile

    {"locale":"en-US","tiles":[{"id":5,"url":"https://www.mozilla.org/en-US/about/"},{"url":"https://twitter.com/Firefox"},{"id":2},{"id":3},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}]}

Note the empty tile at position 7

    {"locale":"en-US","tiles":[{"id":5,"url":"https://www.mozilla.org/en-US/about/"},{"id":2},{"id":3},{"id":6},{"id":8},{"id":9},{"id":10},{"pin":1,"pos":8,"url":"https://twitter.com/Firefox"}]}

The web server listening to requests at the api endpoints will then inject a couple of parameters in the log. These include:
 * timestamp: a unix timestamp at time of receipt, in UTC
 * date: an iso formatted date string for easy splitting by date, in UTC
 * ip: request originator's IPv4 address
 * ua: request originator's UA

An important part of the payload sent by firefox is the `tiles` array. It contains a sequence of JSON objects with data about what was clicked.
In the case of a non-impression payload, there will be an action included, which manifests itself as a top-level key to the payload. The value to this key is an integer. This represents the array index in the `tiles` array that this action is about.

e.g.

    {"locale":"en-US","tiles":[{"id":2}],"click":0}

Means that a user clicked on tile at position 0 in `tiles`. That tile happens to be a directory tile with `id` 2.

__Note__: There can only be one action key

This key can be one of:
 * click
 * pin
 * unpin
 * block

## Tile Object

The payload from Firefox includes the `tiles` key which has as value an array of tile objects. This section describes the schema for tile objects found in this array.

These are the parameters a tile object can have:

 * id
 * url
 * pin
 * pos

### id
Value: Integer  
A tile id.

This is sent if the tile is a directory tile or an enhanced history tile. If it is strictly a history tile, this key won't be included in the tile object.

### url
Value: String  
The url of the tile if it is a history tile.

If it isn't a history tile (i.e. it is a directory tile), this key is not included in the tile object.

### pin
Value: Integer  
Status about this tile

If this tile is pinned in firefox, the value of this key will be `1`. otherwise it won't be included in the tile object.

### pos
Value: Integer  
The position of the tile in the new tab page

this parameter is only included if the index in the array is not a meaningful representation of the tiles.
If all tiles are contiguous in the Firefox new tab page (i.e. there are no gaps), this won't be included in the tile object.

e.g. if a user only has 2 tiles, but happens to have dragged a tile to position 9 and has pinned it, there would be a gap between the tile at index 0 and the last tile, which would be at index 1, but ends up being at position 9.

imagine the user having those tiles:

    tiles: [{id: 1}, {id: 2}]

the user drags tile id 2, and moves it to position 9

    tiles: [{id: 1}, {id: 2, pos: 9}]
