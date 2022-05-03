var sm = new (require('@mapbox/sphericalmercator'))();
var Stats = require('./stream-util').Stats;
var Tile = require('./stream-util').Tile;
var limitBounds = require('./stream-util').limitBounds;
var Info = require('./stream-util').Info;
var isEmpty = require('./stream-util').isEmpty;
var multiread = require('./stream-util').multiread;
var getTileRetry = require('./stream-util').getTileRetry;
var Readable = require('stream').Readable;
var util = require('util');
var validate = require('./tilelive.js').validate;

module.exports = ProjectedScheme;
util.inherits(ProjectedScheme, Readable);

function ProjectedScheme(source, options) {
  if (!source) throw new TypeError('Tilesource required');

  options = options || {};

  if (typeof options.metatile === 'number' && options.metatile <= 0) throw new TypeError('Invalid metatile size');

  if (!options.tilegrid) throw new TypeError('Not a tilegrid');
  if (typeof (options.tilegrid) === 'string') options.tilegrid = JSON.parse(options.tilegrid);

  if (options.tilegrid.bounds !== undefined && !Array.isArray(options.tilegrid.bounds))
    throw new TypeError('options.tilegrid.bounds must be an array of the form [w,s,e,n]');
  if (options.minzoom !== undefined && typeof options.minzoom !== 'number')
    throw new TypeError('options.minzoom must be a positive integer');
  if (options.maxzoom !== undefined && typeof options.maxzoom !== 'number')
    throw new TypeError('options.maxzoom must be a positive integer');
  if (options.minzoom < 0) throw new TypeError('minzoom must be >= 0');
  if (options.minzoom > options.maxzoom) throw new TypeError('maxzoom must be >= minzoom');

  if (!options.tilegrid.srid) throw new TypeError('Missing Tilegrid SRID');
  if (options.tilegrid.srid == 900913 || options.tilegrid.srid == 3857) throw new TypeError('Use --scheme=scanline for 900913/3857')

  if (!options.bbox) options.bbox = options.tilegrid.bounds;
  if (!Array.isArray(options.bbox) || options.bbox.length !== 4) throw new TypeError('options.bbox must be an array of the form [w,s,e,n]');

  if (options.bbox[0] < options.tilegrid.bounds[0]) throw new TypeError('options.bbox[0] has invalid west value');
  if (options.bbox[1] < options.tilegrid.bounds[1]) throw new TypeError('options.bbox[1] has invalid south value');
  if (options.bbox[2] > options.tilegrid.bounds[2]) throw new TypeError('options.bbox[2] has invalid east value');
  if (options.bbox[3] > options.tilegrid.bounds[3]) throw new TypeError('options.bbox[3] has invalid north value');

  if (options.bbox[0] > options.bbox[2]) throw new TypeError('options.bbox[0] cannot be greater than options.bbox[2]');
  if (options.bbox[1] > options.bbox[3]) throw new TypeError('options.bbox[1] cannot be greater than options.bbox[3]');

  if (options.maxzoom >= options.tilegrid.resolutions.length) throw new TypeError('maxzoom must be <= ' + (options.tilegrid.resolutions.length - 1));

  this.type = 'projected'
  this.tilegrid = options.tilegrid;
  this.concurrency = options.concurrency || 8;
  this.metatile = (options.metatile || 1) | 0;

  this.source = source;
  // this.bounds = options.bounds;
  this.minzoom = options.minzoom;
  this.maxzoom = options.maxzoom;
  this.stats = new Stats();
  this.bboxes = undefined;
  this.cursor = undefined;
  this.length = 0;
  this.job = options.job || false;
  this.retry = options.retry || 0;

  // Precalculate the tile int bounds for each zoom level.
  this.bounds = {};
  var tileSize = this.tilegrid.tileSize || 256;

  for (var z = options.minzoom; z <= options.maxzoom; z++) {
    var resolution = this.tilegrid.resolutions[z];

    var ll = [options.bbox[0], options.bbox[1]];
    var ur = [options.bbox[2], options.bbox[3]];
    var px_ll = [(ll[0] - this.tilegrid.origin[0]) / resolution, (this.tilegrid.origin[1] - ll[1]) / resolution];
    var px_ur = [(ur[0] - this.tilegrid.origin[0]) / resolution, (this.tilegrid.origin[1] - ur[1]) / resolution];

    var bounds = {
      minX: Math.floor(px_ll[0] / tileSize),
      minY: Math.floor(px_ur[1] / tileSize),
      maxX: Math.floor((px_ur[0] - 1) / tileSize),
      maxY: Math.floor((px_ll[1] - 1) / tileSize)
    };

    this.bounds[z] = bounds;
    this.stats.total += (this.bounds[z].maxX - this.bounds[z].minX + 1) *
      (this.bounds[z].maxY - this.bounds[z].minY + 1);
  }

  if (this.metatile > 1) {
    this.pos = {
      z: this.minzoom,
      x: this.bounds[this.minzoom].minX - (this.bounds[this.minzoom].minX % this.metatile) - this.metatile,
      y: this.bounds[this.minzoom].minY - (this.bounds[this.minzoom].minY % this.metatile)
    };
  } else {
    this.pos = {
      z: this.minzoom,
      x: this.bounds[this.minzoom].minX - 1,
      y: this.bounds[this.minzoom].minY
    };
  }

  this.box = [];

  Readable.call(this, { objectMode: true });
}

ProjectedScheme.prototype._params = function (callback) {
  var stream = this;
  stream.source.getInfo(function (err, info) {
    if (err) return stream.emit('error', err);

    stream.bounds = stream.bounds !== undefined ? stream.bounds : info.bounds;
    stream.minzoom = stream.minzoom !== undefined ? stream.minzoom : info.minzoom;
    stream.maxzoom = stream.maxzoom !== undefined ? stream.maxzoom : info.maxzoom;

    if (stream.bounds === undefined) return stream.emit('error', new Error('No bounds determined'));
    if (stream.minzoom === undefined) return stream.emit('error', new Error('No minzoom determined'));
    if (stream.maxzoom === undefined) return stream.emit('error', new Error('No maxzoom determined'));

    stream.bboxes = {};
    var boundsArray = limitBounds(stream.bounds);
    var valid = validate({ bounds: boundsArray });
    if (valid instanceof Error) return stream.emit('error', new Error(valid.message));

    for (var z = stream.minzoom; z <= stream.maxzoom; z++) {
      stream.bboxes[z] = sm.xyz(boundsArray, z);

      if (stream.bboxes[z].minX < 0) stream.bboxes[z].minX = 0;
      if (stream.bboxes[z].minY < 0) stream.bboxes[z].minY = 0;

      stream.stats.total +=
        (stream.bboxes[z].maxX - stream.bboxes[z].minX + 1) *
        (stream.bboxes[z].maxY - stream.bboxes[z].minY + 1);
    }

    stream.cursor = {
      z: stream.minzoom,
      x: stream.bboxes[stream.minzoom].minX,
      y: stream.bboxes[stream.minzoom].minY
    };

    callback(null, info);
  });
};

ProjectedScheme.prototype._read = function (/* size */) {
  var stream = this;

  // Defer gets until info is retrieved and there is a cursor to be used.
  if (!stream.bboxes) return stream._params(function (err, info) {
    if (err) return stream.emit('error', err);
    stream.length = stream.stats.total;
    stream.emit('length', stream.length);
    stream.push(new Info(info));
  });

  multiread(stream, function get(push) {
    if (!stream.cursor) return push(null) && false;
    stream.stats.ops++;
    var z = stream.cursor.z;
    var x = stream.cursor.x;
    var y = stream.cursor.y;
    nextDeep(stream);

    if (stream.job && x % stream.job.total !== stream.job.num)
      return skip();

    getTileRetry(stream.source, z, x, y, stream.retry, stream, function (err, buffer) {
      if (err && !(/does not exist$/).test(err.message)) {
        stream.emit('error', err);
      } else if (err || isEmpty(buffer)) {
        skip();
      } else {
        stream.stats.done++;
        push(new Tile(z, x, y, buffer));
      }
    });

    function skip() {
      stream.stats.skipped++;
      stream.stats.done++;
      // Update length
      stream.length--;
      stream.emit('length', stream.length);
      get(push);
    }

    return true;
  });
};

// Increment a tile cursor to the next position,
// descending zoom levels until maxzoom is reached.
function nextDeep(stream) {
  if (!stream.cursor) return false;
  var cursor = stream.cursor;
  cursor.x++;
  var bbox = stream.bboxes[cursor.z];
  if (cursor.x > bbox.maxX) {
    cursor.x = bbox.minX;
    cursor.y++;
  }
  if (cursor.y > bbox.maxY) {
    cursor.z++;
    if (cursor.z > stream.maxzoom) {
      stream.cursor = false;
      return false;
    }
    bbox = stream.bboxes[cursor.z];
    cursor.x = bbox.minX;
    cursor.y = bbox.minY;
  }
  return true;
}
