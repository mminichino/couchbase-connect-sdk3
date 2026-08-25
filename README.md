# couchbase-connect-sdk3

Java helper around the Couchbase Java SDK 3.x for connecting to **Couchbase Server** and **Couchbase Capella**. It wraps cluster connect, bucket/scope/collection management, GSI and search indexes, KV and N1QL access, cluster provisioning, and DCP streaming behind a single `CouchbaseConnect` API.

## Requirements

- Java 17+
- Couchbase Server 7.x+ or Couchbase Capella

## Dependency

**Gradle**

```gradle
implementation 'com.codelry.util:couchbase-connect-sdk3:1.2.5'
```

**Maven**

```xml
<dependency>
  <groupId>com.codelry.util</groupId>
  <artifactId>couchbase-connect-sdk3</artifactId>
  <version>1.2.5</version>
</dependency>
```

## Connecting

`CouchbaseConnect.getInstance()` routes to Capella when `capella.token` is set, otherwise to Couchbase Server. You can also use `Server.getInstance()` or `Capella.getInstance()` directly.

### Couchbase Server

```java
CouchbaseConnect db = CouchbaseConnect.getInstance();
CouchbaseConfig config = new CouchbaseConfig()
    .host("127.0.0.1")
    .username("Administrator")
    .password("password")
    .bucket("data")
    .scope("data")
    .collection("userdata")
    .ssl(false)
    .maxHttpConnections(64);

db.connect(config);
```

### Couchbase Capella

Capella requires an API token plus project and database identifiers. The connection string is resolved through the Capella API.

```java
CouchbaseConnect db = CouchbaseConnect.getInstance();
CouchbaseConfig config = new CouchbaseConfig()
    .username("developer")
    .password("secret")
    .project("my-project")
    .database("my-database")
    .userId("capella-user-id")
    .token("capella-api-token")
    .maxHttpConnections(64);

db.connect(config);
```

### Properties file

```properties
couchbase.hostname=127.0.0.1
couchbase.username=Administrator
couchbase.password=password
couchbase.bucket=data
couchbase.scope=data
couchbase.collection=userdata
couchbase.sslMode=false
couchbase.maxHttpConnections=64
```

For Capella, include the Capella keys instead of (or in addition to) hostname:

```properties
capella.organization.name=My Org
capella.project.name=my-project
capella.database.name=my-database
capella.token=...
capella.user.id=...
couchbase.username=developer
couchbase.password=secret
couchbase.bucket=data
couchbase.scope=data
couchbase.collection=userdata
```

```java
Properties properties = new Properties();
properties.load(Files.newInputStream(Path.of("couchbase.properties")));
CouchbaseConfig config = new CouchbaseConfig().fromProperties(properties);
CouchbaseConnect db = CouchbaseConnect.getInstance();
db.connect(config);
```

## Typical data-path usage

After `connect()`, create the keyspace, wait for services, then read and write documents:

```java
db.createBucket("data");
db.createScope("data", "app");
db.createCollection("data", "app", "users");
db.clusterWait();

db.createPrimaryIndex("data", "app", "users");
db.createSecondaryIndex("data", "app", "users", "idx_email", List.of("email"));

db.connectKeyspace("data", "app", "users");
db.upsert("user::1", Map.of("email", "ada@example.com"));
JsonNode doc = db.get("user::1");
List<JsonNode> rows = db.query("SELECT META().id, email FROM `data`.`app`.`users`");

db.disconnect();
```

Index replica count defaults to `getIndexNodeCount() - 1` (floored at `0`) when you do not pass an explicit replica count. Pass a replica count to override:

```java
db.createPrimaryIndex("data", "app", "users", 1);
```

## Provisioning a cluster

### Couchbase Server

```java
CouchbaseConnect db = Server.getInstance();
CouchbaseConfig config = new CouchbaseConfig()
    .host("127.0.0.1")
    .username("Administrator")
    .password("password")
    .ssl(false);

Map<String, String> options = Map.of(
    "couchbase.server.0.ip", "127.0.0.1",
    "couchbase.server.0.ram", "4",
    "couchbase.server.0.services", "data,index,query,fts"
);

db.createCluster(config, options);
db.connect(config);
```

Additional nodes use `couchbase.server.1.ip`, `couchbase.server.1.services`, and so on. Service RAM quotas can be set with `couchbase.server.<service>.quota`.

### Capella

```java
CouchbaseConnect db = Capella.getInstance();
CouchbaseConfig config = new CouchbaseConfig().fromProperties(properties);

db.createCluster(config);
db.connect(config);
// ...
db.destroyCluster();
db.disconnect();
```

Optional Capella create settings:

| Property | Description |
| --- | --- |
| `capella.cluster.allow` | Allowed CIDR (default `0.0.0.0/0`) |
| `capella.cluster.node.0.cpu` | Node CPU |
| `capella.cluster.node.0.ram` | Node RAM (GiB) |
| `capella.cluster.node.0.services` | Services, e.g. `data,query,index,search` |

## DCP streaming

```java
CouchbaseStream stream = db.stream("data", "app", "users");
stream.streamData().forEach(json -> {
  // each mutation as a JSON string
});
stream.stop();
```

Call `stream.stop()` when finished. `getByCount(n)` blocks until `n` documents are received.

## Configuration reference

Fluent setters on `CouchbaseConfig` match the property names below.

| Property | Default | Description |
| --- | --- | --- |
| `couchbase.hostname` | `127.0.0.1` | Server hostname |
| `couchbase.username` | `Administrator` | Cluster username |
| `couchbase.password` | `password` | Cluster password |
| `couchbase.bucket` | `default` | Bucket name |
| `couchbase.scope` | `_default` | Scope name |
| `couchbase.collection` | `_default` | Collection name |
| `couchbase.sslMode` | `true` | Use TLS (`couchbases://`) |
| `couchbase.replicaNum` | `1` | Bucket replica count |
| `couchbase.kvEndpoints` | `8` | KV connections per node |
| `couchbase.kvTimeout` | `5` | KV timeout (seconds) |
| `couchbase.connectTimeout` | `15` | Connect timeout (seconds) |
| `couchbase.queryTimeout` | `75` | Query timeout (seconds) |
| `couchbase.maxHttpConnections` | `64` | Max HTTP connections for management/query I/O |
| `couchbase.ttlSeconds` | `0` | Default document TTL |
| `couchbase.bucketType` | `couchbase` | `couchbase`, `ephemeral`, or `memcached` |
| `couchbase.storageBackend` | `couchstore` | `couchstore` or `magma` |
| `couchbase.client.cert` | | Client certificate keystore path |
| `couchbase.ca.cert` | | Trusted CA certificate path |
| `couchbase.keystore.type` | `PKCS12` | Keystore type |
| `couchbase.quickConnect` | `false` | Skip wait-until-ready and cluster info |
| `couchbase.softFailure` | `false` | Log connect errors instead of throwing |
| `couchbase.debug` | `false` | Enable debug logging |
| `capella.token` | | Capella API token (selects the Capella driver) |
| `capella.organization.name` / `capella.organization.id` | | Capella organization |
| `capella.project.name` / `capella.project.id` | | Capella project |
| `capella.database.name` / `capella.database.id` | | Capella database |
| `capella.user.email` / `capella.user.id` | | Capella user |
| `capella.api.host` | | Override Capella API host |

`maxHttpConnections` is applied to both Server (`IoConfig`) and Capella (`CapellaClusterConfig`).

## Accessing the SDK

The wrapper exposes the underlying SDK objects when you need them:

```java
Cluster cluster = db.getCluster();
Bucket bucket = db.getBucket();
Collection collection = db.getCollection();
ReactiveCollection reactive = db.getReactiveCollection();
```
