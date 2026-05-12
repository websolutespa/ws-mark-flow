## postgres + pgvector

```
docker pull pgvector/pgvector:pg18-trixie
docker volume create pgvector
docker run -d --name pgvector -p 5432:5432 -e POSTGRES_USER=yourusername -e POSTGRES_PASSWORD=yourpassword -e POSTGRES_DB=yourdatabase -v pgvector:/var/lib/postgresql pgvector/pgvector:pg18-trixie
```

## MongoDB Atlas Vector Search

Requires MongoDB Atlas (cloud) or [Atlas Local](https://www.mongodb.com/docs/atlas/cli/current/atlas-cli-deploy-local/).
Docker image: mongodb/mongodb-atlas-local 
Plain self-hosted MongoDB does NOT support `$vectorSearch`.

Atlas Local (single node) via the Atlas CLI:

```
docker run -d \
  --name mongodb \
  --hostname mongodb \
  -e MONGODB_INITDB_ROOT_USERNAME=root \
  -e MONGODB_INITDB_ROOT_PASSWORD=WS-1234-secret \
  -p 27017:27017 \
  -v ./init:/docker-entrypoint-initdb.d \
  -v mongo-data:/data/db \
  -v mongo-dump:/data/dump \
  -v mongo-config:/data/configdb \
  -v mongot:/data/mongot \
  mongodb/mongodb-atlas-local
```

Configuration fields (UI):

- `uri` — `mongodb+srv://user:pass@cluster.mongodb.net` (or `mongodb://...:27017` for Atlas Local)
- `database` — database name
- `collection` — collection name (default: `ws_mark_flow_chunks`)
- `embedding_dim` — must match the embedding model (e.g. `1536` for `text-embedding-3-small`)
- `index_name` — Atlas vector search index name (default: `vector_index`)
- `similarity` — `cosine` (default), `euclidean`, or `dotProduct`

The integration auto-creates the vector search index on first connect using
`createSearchIndexes` with these filter fields declared: `namespace`, `doc_id`.
Index creation is best-effort (non-fatal if it already exists or the server is
not Atlas).

## Redis Vector Search

[Dashboard](http://localhost:8001) (default credentials: `default/WS-1234-secret` - change on first login)
```
docker volume create redis-data
docker run -d --name redis -e REDIS_ARGS="--requirepass WS-1234-secret" -p 6379:6379 -p 8001:8001 -v redis-data:/data redis/redis-stack:latest
```

## Neo4j Graph Database

[Dashboard](http://localhost:7474) (default credentials: `neo4j/WS-1234-secret` - change on first login)

```
docker pull neo4j:enterprise
docker volume create neo4j-data
docker volume create neo4j-plugins
#enterprise
docker run  --name neo4j -e NEO4J_ACCEPT_LICENSE_AGREEMENT=yes -e NEO4J_AUTH=neo4j/WS-1234-secret -e NEO4J_PLUGINS='["apoc", "apoc-extended", "graph-data-science"]'   -p 7474:7474 -p 7687:7687 -v neo4j-data:/data:rw  -v neo4j-plugins:/plugins:rw -d neo4j:enterprise
```
### ui injestion job

Then in the UI, create an Ingestion job with vector store neo4j, enable Graph Extraction → schema_guided, and paste an ontology like:

```yaml
node_labels: [Person, Organization, Product]
relations:
  - {type: WORKS_AT,  source: [Person],       target: [Organization]}
  - {type: PRODUCES,  source: [Organization], target: [Product]}
node_properties:
  Person: [role]
  Organization: [country]
```

Useful Cypher queries for debugging:

```
// All entities for a namespace
MATCH (e:Entity {namespace:'default'}) RETURN e.label, count(*) ORDER BY count(*) DESC;

// GraphRAG-style expansion: vector top-k → entities → 2-hop neighbors
CALL db.index.vector.queryNodes('ws_mark_flow_chunk_embeddings', 8, $vec)
YIELD node, score
WHERE node.namespace = 'default'
WITH node LIMIT 5
MATCH (node)-[:MENTIONS]->(e:Entity)-[*1..2]-(nbr:Entity)
RETURN DISTINCT e.label, e.name, collect(DISTINCT nbr.name)[0..10] AS neighbors;

// Wipe a single doc (chunks + Document, keeps shared entities)
MATCH (n) WHERE (n:Chunk OR n:Document) AND n.namespace='default' AND n.doc_id=$id
DETACH DELETE n;
```