'use strict';
import {EventEmitter} from 'events';
import Memcached, {VersionData} from 'memcached';
import _ from 'lodash';
export = MemcachedElasticache;

Promise = require('bluebird');

const GET_CLUSTER_COMMAND_OLD = 'get AmazonElastiCache:cluster';
const GET_CLUSTER_COMMAND_NEW = 'config get cluster';

const DEFAULT_AUTO_DISCOVER = true;
const DEFAULT_AUTO_DISCOVER_INTERVAL = 60000;
const DEFAULT_AUTO_DISCOVER_OVERRIDES_REMOVE = false;

declare namespace MemcachedElasticache {
	interface options extends Memcached.options {
		autoDiscover?: boolean;
		autoDiscoverInterval?: number;
		autoDiscoverOverridesRemove?: boolean;
	}
}

function getOption(options: MemcachedElasticache.options, name: string, defaultValue: any) {
	if (_.has(options, name)) {
		return options[name];
	} else {
		return defaultValue;
	}
}

function deleteOption(options: MemcachedElasticache.options, name: string) {
	if (_.has(options, name)) {
		delete options[name];
	}
}

class MemcachedElasticache extends EventEmitter {
	private _options: MemcachedElasticache.options;
	private _nodeSet: Set<string>;
	private _configEndpoint: string;
	private _timer: NodeJS.Timeout | null;
	private _innerClient: Memcached;

    constructor(configEndpoint: string, options: MemcachedElasticache.options = {}) {
        super();

		// extract outer client options so they aren't passed to inner client
		const autoDiscover = getOption(options, 'autoDiscover', DEFAULT_AUTO_DISCOVER);
		const autoDiscoverInterval = getOption(options, 'autoDiscoverInterval', DEFAULT_AUTO_DISCOVER_INTERVAL);
		const autoDiscoverOverridesRemove = getOption(options, 'autoDiscoverOverridesRemove', DEFAULT_AUTO_DISCOVER_OVERRIDES_REMOVE);
		this._options = _.clone(options);
		deleteOption(this._options, 'autoDiscover');
		deleteOption(this._options, 'autoDiscoverInterval');
		deleteOption(this._options, 'autoDiscoverOverridesRemove');

        this._configEndpoint = configEndpoint;
        this._nodeSet = new Set();

		// keep our set of nodes in sync with the inner client's set of nodes should it remove a node
		if (autoDiscoverOverridesRemove) {
			this.on('remove', (details) => {
				this._nodeSet.delete(details.server);
			});
		}

        // when auto-discovery is enabled, the configuration endpoint is a valid
        // cluster node so use it to for the initial inner client until cluser
        // discovery is complete and the inner client is replaced/updated with
        // all the nodes in the cluster; when auto-discovery is disabled, the
        // inner client never changes and this class is just a dumb wrapper
        this._createInnerClient(configEndpoint);

        // start auto-discovery, if enabled
        if (autoDiscover) {
            this._getCluster();
            this._timer = setInterval(this._getCluster.bind(this), autoDiscoverInterval);
        }
    }

	// passthrough method calls from outer object to inner object - except
	// end(), which we explicitly override

	touch(key: string, lifetime: number, cb: (this: Memcached.CommandData, err: any) => void): void {
    	this._innerClient.touch(key, lifetime, cb);
	}

	get(key: string, cb: (this: Memcached.CommandData, err: any, data: any) => void): void {
    	this._innerClient.get(key, cb);
	}

	gets(key: string, cb: (this: Memcached.CommandData, err: any, data: {[key: string]: any, cas: string}) => void): void {
    	this._innerClient.gets(key, cb);
	}

	getMulti(keys: string[], cb: (this: undefined, err: any, data: {[key: string]: any}) => void): void {
    	this._innerClient.getMulti(keys, cb);
	}

	set(key: string, value: any, lifetime: number, cb: (this: Memcached.CommandData, err: any, result: boolean) => void): void {
    	this._innerClient.set(key, value, lifetime, cb);
	}

	replace(key: string, value: any, lifetime: number, cb: (this: Memcached.CommandData, err: any, result: boolean) => void): void {
    	this._innerClient.replace(key, value, lifetime, cb);
	}

	add(key: string, value: any, lifetime: number, cb: (this: Memcached.CommandData, err: any, result: boolean) => void): void {
    	this._innerClient.add(key, value, lifetime, cb);
	}

	cas(key: string, value: any, cas: string, lifetime: number, cb: (this: Memcached.CommandData, err: any, result: boolean) => void): void {
    	this._innerClient.cas(key, value, cas, lifetime, cb);
	}

	append(key: string, value: any, cb: (this: Memcached.CommandData, err: any, result: boolean) => void): void {
    	this._innerClient.append(key, value, cb);
	}

	prepend(key: string, value: any, cb: (this: Memcached.CommandData, err: any, result: boolean) => void): void {
    	this._innerClient.prepend(key, value, cb);
	}

	incr(key: string, amount: number, cb: (this: Memcached.CommandData, err: any, result: boolean|number) => void): void {
    	this._innerClient.incr(key, amount, cb);
	}

	decr(key: string, amount: number, cb: (this: Memcached.CommandData, err: any, result: boolean|number) => void): void {
    	this._innerClient.decr(key, amount, cb);
	}

	del(key: string, cb: (this: Memcached.CommandData, err: any, result: boolean) => void): void {
    	this._innerClient.del(key, cb);
	}

	version(cb: (err: any, version: Memcached.VersionData[]) => void): void {
    	this._innerClient.version(cb);
	}

	settings(cb: (err: any, settings: Memcached.StatusData[]) => void): void {
    	this._innerClient.settings(cb);
	}

	stats(cb: (err: any, stats: Memcached.StatusData[]) => void): void {
    	this._innerClient.stats(cb);
	}

	slabs(cb: (err: any, stats: Memcached.StatusData[]) => void): void {
    	this._innerClient.stats(cb);
	}

	items(cb: (err: any, stats: Memcached.StatusData[]) => void): void {
    	this._innerClient.items(cb);
	}

	cachedump(server: string, slabid: number, number: number, cb: (err: any, cachedump: Memcached.CacheDumpData|Memcached.CacheDumpData[]) => void): void {
    	this.cachedump(server, slabid, number, cb);
	}

	flush(cb: (this: undefined, err: any, results: boolean[]) => void): void {
    	this.flush(cb);
	}

	end() {

        // stop auto-discovery
        if (this._timer) {
            clearInterval(this._timer);
            this._timer = null;
        }

        this._innerClient.end();
    }

    private _getCluster() {

        // connect to configuration endpoint
        const configClient = new Memcached(this._configEndpoint, {
			// attempt to contact server 3 times in 3 seconds before marking it dead
			timeout: 1000,
			retries: 2,
			failures: 0
		});

        new Promise((resolve, reject) => {

            // get cache engine version
            configClient.version((err, version) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(version);
                }
            });
        })
        .then((version: VersionData[]) => {

            // select cluster command based on cache engine version
            const major = parseInt(version[0].major);
            const minor = parseInt(version[0].minor);
            const bugfix = parseInt(version[0].bugfix);
            const clusterCommand =
                (major > 1) || (major === 1 && minor > 4) || (major === 1 && minor === 4 && bugfix >= 14) ?
                GET_CLUSTER_COMMAND_NEW : GET_CLUSTER_COMMAND_OLD;

            // request nodes from configuration endpoint
            return new Promise((resolve, reject) => {
                // @ts-ignore
				configClient.command(() => {
                    return {
                        command: clusterCommand,
                        callback: (err, data) => {
                            if (err) {
                                reject(err);
                            } else {
                                resolve(data);
                            }
                        }
                    };
                });
            });
        })
        .then((data) => this._parseNodes(data))
        .then((nodes) => {

			// update inner client only if nodes have changed
			const nodeSet = new Set(nodes);
            if (!_.isEqual(this._nodeSet, nodeSet)) {
                this._nodeSet = nodeSet;
                this._createInnerClient(nodes);
                this.emit('autoDiscover', nodes);
            }
            configClient.end();
        })
        .catch((err) => {
            this.emit('autoDiscoverFailure', err);
            configClient.end();
        })
    }

    private _parseNodes(data): string[] {
		const lines = data.split('\n');
		const nodes = lines[1].split(' ').map((entry) => {
            const parts = entry.split('|');
            return `${parts[0]}:${parts[2]}`;
        });

		// make sure node order is consistent so key hashing is consistent
        return nodes.sort();
	}

    private _createInnerClient(servers) {
        // (re)create inner client object - do not call end() on previous inner
        // client as this will cancel any in-flight operations
        this._innerClient = new Memcached(servers, this._options);

        // passthrough emitted events from inner object to outer object
        this._innerClient.emit = this.emit.bind(this);
    }
}

