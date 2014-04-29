var sm = new (require('sphericalmercator'));
var ScanlineScheme = require('./scanlinescheme');
var Tile = require('./tile').Tile;
var Metatile = require('./tile').Metatile;
var unserializeTiles = require('./tile').unserialize;
var Statistics = require('./statistics');

module.exports = ProjectedScheme;
require('util').inherits(ProjectedScheme, ScanlineScheme);
function ProjectedScheme(options) {
    this.type = 'projected';

    if (!options.tilegrid) throw new Error('Bad Tilegrid');
    if (typeof(options.tilegrid) === 'string') {
        options.tilegrid = JSON.parse(options.tilegrid);
    }
    if (!options.tilegrid.srid) throw new Error('Bad Tilegrid');
    if (options.tilegrid.srid == 900913 || options.tilegrid.srid == 3857) throw new Error('Use --scheme=scanline for 900913/3857')

    if (!options.bbox) options.bbox = options.tilegrid.bounds;
    if (!Array.isArray(options.bbox) || options.bbox.length !== 4) throw new Error('bbox must have four coordinates');
    if (options.bbox[0] < options.tilegrid.bounds[0]) throw new Error('bbox has invalid west value');
    if (options.bbox[1] < options.tilegrid.bounds[1]) throw new Error('bbox has invalid south value');
    if (options.bbox[2] > options.tilegrid.bounds[2]) throw new Error('bbox has invalid east value');
    if (options.bbox[3] > options.tilegrid.bounds[3]) throw new Error('bbox has invalid north value');
    if (options.bbox[0] > options.bbox[2]) throw new Error('bbox is invalid');
    if (options.bbox[1] > options.bbox[3]) throw new Error('bbox is invalid');
    if (typeof options.minzoom !== 'number') throw new Error('minzoom must be a number');
    if (typeof options.maxzoom !== 'number') throw new Error('maxzoom must be a number');
    if (options.minzoom < 0) throw new Error('minzoom must be >= 0');
    if (options.maxzoom >= options.tilegrid.resolutions.length) throw new Error('maxzoom must be <= ' + (options.tilegrid.resolutions.length-1));
    if (options.minzoom > options.maxzoom) throw new Error('maxzoom must be >= minzoom');
    if (typeof options.metatile === 'number' && options.metatile <= 0) throw new Error('Invalid metatile size');

    this.tilegrid = options.tilegrid;
    this.concurrency = options.concurrency || 8;
    this.minzoom = options.minzoom;
    this.maxzoom = options.maxzoom;
    this.metatile = (options.metatile || 1) | 0;

    // Precalculate the tile int bounds for each zoom level.
    this.bounds = {};
    this.stats = new Statistics();
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

    this.initialize();
}

ProjectedScheme.unserialize = function(state) {
    var scheme = Object.create(ProjectedScheme.prototype);
    for (var key in state) scheme[key] = state[key];
    scheme.stats = Statistics.unserialize(state.stats);
    scheme.initialize();
    return scheme;
};

ProjectedScheme.prototype.toJSON = function() {
    var o = ProjectedScheme.super_.prototype.toJSON.apply(this);
    o.tilegrid = this.tilegrid;
    return o;
};

