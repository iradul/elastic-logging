# Elastic-Log
Log directly to Elasticsearch. Library can handle high write rate beacuse it's using bulk API.

## Example

```javascript
(async function () {
    const el = new Logger({
        /**Elasticsearch host */
        esHost: '192.168.7.120:9200',
        /**Elasticsearch request timeout in milliseconds. Default is `30000`. */
        esRequestTimeout_ms: 5000,
        /**Flush interval in milliseconds. Default is `5000`. */
        flushInterval_ms: 900,
        /**Interval in seconds at which new index with suffix gets created.
         * Suffix is calculated as current timestamp divided by this interval
         * at the moment of `log` method call.
         * When set to `0` index will not get any suffix.
         * 
         * Default is `3600` (1 hour).*/
        indexSplitInterval_sec: 60,
        /**Log errors on stderr. Default is `true`. */
        logErrors: false,
    });

    // instance must be initialized
    await el.initialize();

    const mappingProps = { date: { type: "date" }};
    const msg1 = {
        date: Date.now(),
        ip: "192.168.1.102",
        retries: 8,
    };
    el.log("status", msg1, mappingProps);

    // make sure messages get written to elastic
    await el.flush();

    const msg2 = {
        date: Date.now() + 123,
        ip: "192.168.1.101",
        retries: 4,
    };
    el.log("status", msg2, mappingProps);

    // make sure to gracefully close
    await el.close();
})();
```
