This class duplicates the logic that elasticsearch uses to route a document to a particular partition. I'm using this library in Spark to force the output to be partitioned in a way that can be fed into our custom Elasticsearch ingester.

### Usage
Assuming that your index has 70 shards and your document_id is "foobar123", you can determine the correct shard for your document via: 
```
es_hasher = EsMurmur3(70)
es_hasher.hash("foobar123") # => 8
``

