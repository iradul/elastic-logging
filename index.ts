import { httpreq } from 'h2tp';

export interface IField {
    type: "text" | "keyword" | "date" | "long" | "double" | "boolean" | "ip" | "object" | "nested" | "geo_point" | "geo_shape" | "completion";
}

export type Mappings = {
    [field: string]: IField
}

interface ESIndex {
    bulk: string;
    bulkSize: number;
    created: number;
}

interface ESInfo {
    name : string;
    cluster_name : string;
    cluster_uuid : string;
    version : {
      number : string;
      build_flavor : string;
      build_type : string;
      build_hash : string;
      build_date : string;
      build_snapshot : boolean;
      lucene_version : string;
      minimum_wire_compatibility_version : string;
      minimum_index_compatibility_version : string;
    },
    tagline : string;
  }
  

/**
 * Application logging system
 * Logs to Elasticsearch
 */
export class Logging {
    public logErrors = true;
    public elasticBulkInterval = 5000;
    public elasticRequestTimeout = 4000;
    public cleanUpInterval = 3600000;
    private dying = false;
    private bulkTID: any;
    private cleanTID: any;
    private indexes: { [name: string]: ESIndex } = {};
    private todo: ESIndex[] = [];
    private esVersion!: { major: number, minor: number, patch: number };

    constructor(public elasticServer: string) {
        this.elasticPush();
        this.cleanUp();
        const die = () => {
            this.elasticPush();
            this.dying = true;
        }
        process.on('SIGINT', die);
        process.on('SIGTERM', die);
    }

    public load() {
        return httpreq(`http://${this.elasticServer}`)
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
                    return Promise.reject(`Invalid elasticsearch server:\n${JSON.stringify(info, null, 4)}`);
                }
                return Promise.reject(`Got server contacting elasticsearch server`);
            })
            .catch(err => {
                return Promise.reject(`Can't connecto to the server`);
            })
    }

    public logt(index: string, message: any, mappings?: Mappings, type = "doc") {
        return this.log(this.elasticIndexWithTime(index), message, mappings);
    }

    public log(index: string, message: any, mappings?: Mappings, type = "doc") {
        if (this.dying) return;
        const esindex = this.indexes[index];
        if (!esindex) {
            const promiseIndex = (!mappings) ? Promise.resolve() : this.createIndex(index, type, JSON.stringify(mappings));
            promiseIndex.then(() => {
                const newESindex = {
                    bulk: '',
                    bulkSize: 0,
                    created: (new Date()).getTime(),
                };
                this.indexes[index] = newESindex;
                this.todo.push(newESindex);
                this.log(index, message, mappings, type);
            }, (e) => this.handleError(e));
        } else {
            esindex.bulk += (this.esVersion.major >= 7)
                ? `{"index":{"_index":"${index}"}}\n`
                : `{"index":{"_index":"${index}","_type":"${type}"}}\n`;
            esindex.bulk += JSON.stringify(message) + '\n';
            esindex.bulkSize++;
        }
    }

    private elasticIndexWithTime(baseIndex: string) {
        const today = (new Date()).toISOString().replace(/T.*/, '').replace(/-/g, '.');
        return `${baseIndex}-${today}`
    }

    private elasticPush() {
        clearTimeout(this.bulkTID);
        let promiseDone: Promise<void>;
        if (this.todo.length === 0) {
            promiseDone = Promise.resolve();
        } else {
            const all: Promise<void>[] = [];
            this.todo.forEach(esindex => {
                if (esindex.bulkSize > 0) {
                    all.push(
                        httpreq({
                            url: `http://${this.elasticServer}/_bulk`,
                            method: 'POST',
                            payload: esindex.bulk,
                            headers: { "Content-Type": "application/x-ndjson" },
                            timeout: this.elasticRequestTimeout,
                        }).then(r => {
                            return (r.response.statusCode === 200 && (/"errors":(true|false)/.exec(r.body) || '')[1] === "false") ? Promise.resolve() : Promise.reject(new Error(r.body));
                        })
                    );
                    esindex.bulk = '';
                    esindex.bulkSize = 0;
                }
            });
            this.todo = [];
            promiseDone = Promise.all(all).then(() => Promise.resolve());
        }
        return promiseDone.then(() => {
            this.bulkTID = setTimeout(() => this.elasticPush(), this.elasticBulkInterval);
            return Promise.resolve();
        }, e => {
            this.handleError(e);
            this.bulkTID = setTimeout(() => this.elasticPush(), this.elasticBulkInterval);
            return Promise.resolve();
        });
    }

    private createIndex(index: string, type: string, properties: string): Promise<void> {
        const
            indexUrl = `http://${this.elasticServer}/${index}`,
            mappingUrl = (this.esVersion.major >= 7)
                ? `${indexUrl}/_mapping`
                : `${indexUrl}/_mapping/${type}`;
        // check if type exists
        return httpreq({
            url: mappingUrl,
            method: 'GET',
            timeout: this.elasticRequestTimeout,
        }).then((r) => {
            if (r.response.statusCode === 200) {
                // index exists
                return Promise.resolve();
            } else if (r.response.statusCode === 404) {
                // index doesn't exist, create it
                const payload = (this.esVersion.major >= 7)
                    ? `{"mappings":{"properties":${properties}}}`
                    : `{"mappings":{"${type}":{"properties":${properties}}}}`;
                return httpreq({
                    url: indexUrl,
                    method: 'PUT',
                    payload,
                    headers: { "Content-Type": "application/json" },
                    timeout: this.elasticRequestTimeout,
                }).then(r => (r.response.statusCode !== 200) ? Promise.reject("Can't create indice") : Promise.resolve());
            } else {
                // unknown error while checking existence
                return Promise.reject("Can't create indice");
            }
        })
    }

    private cleanUp() {
        this.cleanTID = setTimeout(() => {
            this.cleanUp();
        }, this.cleanUpInterval);
        const mintime = (new Date()).getTime() - this.cleanUpInterval;
        for (const p in this.indexes) {
            const esindex = this.indexes[p];
            if (esindex.created < mintime) delete this.indexes[p];
        }
    }

    private handleError(e: Error) {
        if (this.logErrors) console.log(`elastic-log: ${e.message}\n${e.stack}`);
    }
}
