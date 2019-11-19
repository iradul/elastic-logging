import { httpreq } from 'h2tp';

// https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html
interface IBaseFieldMapping {
    analyzer?: string;
    boost?: number;
    coerce?: boolean;
    copy_to?: string;
    doc_values?: boolean;
    dynamically?: boolean | 'strict';
    enabled?: boolean;
    format?: string;
    ignore_above?: number;
    ignore_malformed?: boolean;
    index_options?: 'docs' | 'freqs' | 'positions' | 'offsets';
    index_prefixes?: {
        min_chars : number;
        max_chars : number;
    };
    index?: boolean;
    null_value?: any;
    properties?: IBaseFieldMapping;
    search_analyzer?: string;
    similarity?: 'BM25' | 'classic' | 'boolean';
    store?: boolean;
    term_vector?: 'no' | 'yes' | 'with_offsets' | 'with_positions' | 'with_positions_offsets' | 'with_positions_payloads' | 'with_positions_offsets_payloads';
    type: "text" | "keyword" | "date" | "long" | "double" | "boolean" | "ip" | "object" | "nested" | "geo_point" | "geo_shape" | "completion";
}

export interface IFieldProps extends IBaseFieldMapping {
    fields?: IBaseFieldMapping;
}

export type MappingProperties = {
    [field: string]: IFieldProps;
}

interface ESBulk {
    bulk: string;
    bulkSize: number;
    obsolete: number;
}

interface ESInfo {
    name: string;
    cluster_name: string;
    cluster_uuid: string;
    version: {
        number: string;
        build_flavor: string;
        build_type: string;
        build_hash: string;
        build_date: string;
        build_snapshot: boolean;
        lucene_version: string;
        minimum_wire_compatibility_version: string;
        minimum_index_compatibility_version: string;
    },
    tagline: string;
}

interface ESVersion {
    major: number;
    minor: number;
    patch: number;
};

export interface Config {
    /**Elasticsearch host */
    esHost: string;
    /**Elasticsearch request timeout in milliseconds. Default is `30000`. */
    esRequestTimeout_ms?: number;
    /**Flush interval in milliseconds. Default is `5000`. */
    flushInterval_ms?: number;
    /**Interval in seconds at which new index with suffix gets created.
     * Suffix is calculated as current timestamp divided by this interval
     * at the moment of `log` method call.
     * When set to `0` index will not get any suffix.
     * 
     * Default is `3600` (1 hour).*/
    indexSplitInterval_sec?: number;
    /**Log errors on stderr. Default is `true`. */
    logErrors?: boolean;
}

/**
 * Elasticsearch bulk logger
 */
export class Logger {
    public esHost: string;
    public logErrors = true;
    public flushInterval = 5000;
    public esRequestTimeout = 30000;
    public indexSplitInterval = 3600000;

    private dying = false;
    private autoFlushTID: any;
    private cleanUpTID: any;
    private indexBulks: Map<string, ESBulk> = new Map();
    private nonEmptyBulks: Set<string> = new Set();
    private esVersion!: ESVersion;
    private pendingMessages = 0;
    private createIndexPromises: Map<string, Promise<ESBulk>> = new Map();

    constructor(config: Config | string) {
        if (typeof(config) === 'string') {
            this.esHost = config;
        } else {
            this.esHost = config.esHost;
            this.flushInterval = (config.flushInterval_ms !== undefined) ? config.flushInterval_ms : this.flushInterval;
            this.esRequestTimeout = (config.esRequestTimeout_ms !== undefined) ? config.esRequestTimeout_ms : this.esRequestTimeout;
            this.indexSplitInterval = (config.indexSplitInterval_sec !== undefined) ? 1000 * config.indexSplitInterval_sec : this.indexSplitInterval;
            this.logErrors = (config.logErrors !== undefined) ? !!config.logErrors : this.logErrors;
        }
        this.autoFlush();
        this.cleanUpOldIndexBulks();
    }

    /**Initialize Logger class */
    public initialize() {
        return httpreq(`http://${this.esHost}`)
            .then(r => {
                if (r.response.statusCode === 200) {
                    const info: ESInfo = JSON.parse(r.body);
                    const rex = /^(\d+)\.(\d+)\.(\d+)/.exec(info.version.number);
                    if (rex) {
                        this.esVersion = {
                            major: +rex[1],
                            minor: +rex[2],
                            patch: +rex[3],
                        }
                        return Promise.resolve();
                    }
                    return Promise.reject(new Error(`Invalid elasticsearch server:\n${JSON.stringify(info, null, 4)}`));
                }
                return Promise.reject(new Error(`Got server contacting elasticsearch server`));
            })
            .catch(err => {
                this.handleError(err);
                return Promise.reject(new Error(`Can't connecto to the server`));
            })
    }

    /**Close logger gracefully. Instance can't be reused after this call. */
    public async close(): Promise<void> {
        this.dying = true;
        clearTimeout(this.autoFlushTID);
        clearTimeout(this.cleanUpTID);
        const delay = 100;
        while (this.pendingMessages > 0) {
            await (new Promise(res => setTimeout(() => res(), delay)));
        }
        return this.flush();
    }

    /**Flush logs to Elasticsearch */
    public flush() {
console.log(`size ${this.nonEmptyBulks.size}`)
        if (this.nonEmptyBulks.size === 0) {
            return Promise.resolve();
        } else {
            const all: Promise<void>[] = [];
            this.nonEmptyBulks.forEach(index => {
                const b = this.indexBulks.get(index);
                if (b && b.bulkSize > 0) {
console.log(`doing push ${b.bulkSize}`)
                    all.push(
                        httpreq({
                            url: `http://${this.esHost}/_bulk`,
                            method: 'POST',
                            payload: b.bulk,
                            headers: { "Content-Type": "application/x-ndjson" },
                            timeout: this.esRequestTimeout,
                        })
                            .then(r => {
                                return (r.response.statusCode !== 200 || /"errors":true/.exec(r.body)) ? Promise.reject(new Error(r.body || `Got status ${r.response.statusCode} from elasticsearch`)) : Promise.resolve();
                            })
                    );
                    b.bulk = '';
                    b.bulkSize = 0;
                }
            });
            this.nonEmptyBulks.clear();
            return Promise.all(all).then(() => Promise.resolve());
        }
    }

    /**Log message to elastic */
    public log(index: string, message: any, properties?: MappingProperties, type = "doc") {
        if (this.dying) return;
        if (!this.esVersion) {
            this.handleError(new Error(`Logger is not initialized`));
            return;
        }
        index = this.elasticIndexWithSufix(index);
        const b = this.indexBulks.get(index);
        if (!b) {
            this.pendingMessages++;
            this.createIndex(index, type, properties)
                .then((b2) => {
                    this.addToBulk(index, b2, message, type);
                    this.pendingMessages--;
                })
                .catch(e => {
                    this.handleError(e);
                    this.pendingMessages--;
                })
        } else {
            this.addToBulk(index, b, message, type);
        }
    }

    private addToBulk(index: string, b: ESBulk, message: any, type: string) {
        this.nonEmptyBulks.add(index);
        b.bulk += (this.esVersion.major >= 7)
            ? `{"index":{"_index":"${index}"}}\n`
            : `{"index":{"_index":"${index}","_type":"${type}"}}\n`;
        b.bulk += JSON.stringify(message) + '\n';
        b.bulkSize++;
    }

    private elasticIndexWithSufix(baseIndex: string) {
        return this.indexSplitInterval === 0 ? baseIndex : `${baseIndex}-${Math.floor(Date.now() / this.indexSplitInterval)}`;
    }

    private autoFlush() {
        if (this.autoFlushTID === null) {
            this.autoFlushTID = setInterval(() => {
                this.flush();
                this.autoFlush();
            }, this.flushInterval);
        }
    }

    private createIndex(index: string, type: string, properties?: MappingProperties): Promise<ESBulk> {
        if (this.createIndexPromises.has(index)) {
            return this.createIndexPromises.get(index)!;
        }
        const
            indexUrl = `http://${this.esHost}/${index}`,
            mappingUrl = (this.esVersion.major >= 7)
                ? `${indexUrl}/_mapping`
                : `${indexUrl}/_mapping/${type}`;

        const resolveESBulk = (obsolete?: number): Promise<ESBulk> => {
            const b: ESBulk = { bulk: '', bulkSize: 0, obsolete: obsolete !== undefined ? obsolete : Date.now() + 2 * this.indexSplitInterval };
            this.indexBulks.set(index, b);
            return Promise.resolve(b);
        };
        // check if type exists
        const promise = !properties
            ? resolveESBulk()
            : httpreq({
                url: mappingUrl,
                method: 'GET',
                timeout: this.esRequestTimeout,
            })
            .then((r) => {
                if (r.response.statusCode === 200) {
                    // index exists
                    return resolveESBulk(this.indexSplitInterval > 0 ? +/\d+$/.exec(index)![0] * this.indexSplitInterval : undefined);
                } else if (r.response.statusCode === 404) {
                    // index doesn't exist, create it
                    const propsJson = JSON.stringify(properties);
                    const payload = (this.esVersion.major >= 7)
                        ? `{"mappings":{"properties":${propsJson}}}`
                        : `{"mappings":{"${type}":{"properties":${propsJson}}}}`;
                    return httpreq({
                        url: indexUrl,
                        method: 'PUT',
                        payload,
                        headers: { "Content-Type": "application/json" },
                        timeout: this.esRequestTimeout,
                    })
                    .then(r => (r.response.statusCode === 200) ? resolveESBulk() : Promise.reject(new Error("Can't create indice")))
                } else {
                    // unknown error while checking existence
                    return Promise.reject(new Error("Can't create indice"));
                }
            })
        promise
            .catch(() => Promise.resolve())
            .then(() => this.createIndexPromises.delete(index))
        this.createIndexPromises.set(index, promise);
        return promise;
    }

    private cleanUpOldIndexBulks() {
        if (this.indexSplitInterval > 0) {
            this.cleanUpTID = setTimeout(() => {
                this.cleanUpOldIndexBulks();
            }, this.indexSplitInterval);
            const mintime = Date.now() - this.indexSplitInterval;
            for (const entries of this.indexBulks.entries()) {
                if (entries[1].obsolete < mintime) {
                    this.indexBulks.delete(entries[0]);
                }
            }
        }
    }

    private handleError(e: Error) {
        if (this.logErrors) console.error(`elastic-log: ${e.message}\n${e.stack}`);
    }
}
