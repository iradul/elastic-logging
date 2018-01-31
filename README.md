# Elastic-Log
Log directly to Elasticsearch.

## Example

```javascript
const el = new Logging("192.168.1.99:9200");
const msgExample = {
    date: (new Date()).getTime(),
    ip: "192.168.1.102",
    retries: 8,
};
// say today is 01/30/2018
// this will create index "status-2018-01-30"
// and add msgExample as document
el.logt("status", msgExample, { date: { type: "date" } });

// this will create index "test"
// and add msgExample as document
el.log("test", msgExample, { date: { type: "date" } });
```
