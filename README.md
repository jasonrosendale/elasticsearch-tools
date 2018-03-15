# elasticsearch-tools

## Murmer

A python class that duplicates the logic that elasticsearch uses to route a document to a particular partition. The elasticsearch algorithm is a combination of the Murmur3 hashing function mixed with some unicode jankery and typecasting.
