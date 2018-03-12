class EsMurmur3:
  def __init__(self, shard_count):
    self.shard_count = shard_count

  # Logic copied from org.elasticsearch.common.hash.MurmurHash3.java
  def __murmur3_x86_32(self, data, seed = 0, length = None):
    c1 = 0xcc9e2d51
    c2 = 0x1b873593
    length = len(data)

    h1 = seed
    roundedEnd = (length & 0xfffffffc)
    for i in range(0, roundedEnd, 4):
      k1 = (ord(data[i]) & 0xff) | ((ord(data[i + 1]) & 0xff) << 8) | \
             ((ord(data[i + 2]) & 0xff) << 16) | (ord(data[i + 3]) << 24)
      k1 *= c1
      k1 = (k1 << 15) | ((k1 & 0xffffffff) >> 17)
      k1 *= c2

      h1 ^= k1
      h1 = (h1 << 13) | ((h1 & 0xffffffff) >> 19)
      h1 = h1 * 5 + 0xe6546b64

      # tail
      k1 = 0

      val = length & 0x03
      if val == 3:
          k1 = (ord(data[roundedEnd + 2]) & 0xff) << 16
      if val in [2, 3]:
          k1 |= (ord(data[roundedEnd + 1]) & 0xff) << 8
      if val in [1, 2, 3]:
          k1 |= ord(data[roundedEnd]) & 0xff
          k1 *= c1
          k1 = (k1 << 15) | ((k1 & 0xffffffff) >> 17)
          k1 *= c2
          h1 ^= k1

      h1 ^= length

    h1 ^= ((h1 & 0xffffffff) >> 16)
    h1 *= 0x85ebca6b
    h1 ^= ((h1 & 0xffffffff) >> 13)
    h1 *= 0xc2b2ae35
    h1 ^= ((h1 & 0xffffffff) >> 16)

    return h1 & 0xffffffff

  # There's certainly a cleaner way to convert an integer to a signed 32-bit int,
  # and perhaps some day I'll have time to find it
  def __signed(self, n):
    if n > 2**31:
      return self.__signed(n - 2**32)
    return n
  
  # Elasticsearch assumes that a document ID is in UTF-16 and converts each 
  # character in the ID to a high and low byte before passing it to the murmur
  # hashing function.
  # Logic copied from org/elasticsearch/cluster/routing/Murmur3HashFunction.java
  def __es_xform(self, routing):
    bytesToHash = list()
    for c in str(routing):
      bytesToHash.append(c)
      bytesToHash.append( chr((ord(c) & 0xffffffff) >> 8) )
    return "".join(bytesToHash)

  # The ES documentation says that the shard is determined by hashing the document
  # id and then taking the result modulo <# of shards>. But that's not precisely
  # true: in org/elasticsearch/cluster/routing/OperationRouting.java we see that
  # we're using java's Math.floorMod method.  
  def __floor_mod(self, n, mod):
    return n - ((n // mod) * mod)

  def hash(self, routing):
    hashval = self.__signed(self.__murmur3_x86_32(self.__es_xform(routing), 0))
    return self.__floor_mod(hashval, self.shard_count)