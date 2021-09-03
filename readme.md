
# simple-redisclient

Redis client using Redis Serialization Protocol


## Implements

Refer to [Redis Serialization Protocol](https://redis.io/topics/protocol?spm=a2c6h.12873639.0.0.4ed41d6aOIOVhK) and [jedis](https://github.com/redis/jedis):

- Server To Client

| Protocol Type | First Byte | Example |
|--------|--------|--------|
| Simple Strings | `+` | `+OK\r\n` |
| Errors  | `-` | `-ERR unknown command 'hello'` |
| Integers  | `:` | `:2` |
| Bulk Strings | `$` | `$6\r\nfoobar\r\n` |
| Arrays  | `*` | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` |

```
// redis.clients.jedis.Protocol#read
public static final byte DOLLAR_BYTE = '$';
public static final byte ASTERISK_BYTE = '*';
public static final byte PLUS_BYTE = '+';
public static final byte MINUS_BYTE = '-';
public static final byte COLON_BYTE = ':';

public static Object read(final RedisInputStream is) {
    return process(is);
}
private static Object process(final RedisInputStream is) {
    final byte b = is.readByte();
    switch (b) {
    case PLUS_BYTE:
      return processStatusCodeReply(is);
    case DOLLAR_BYTE:
      return processBulkReply(is);
    case ASTERISK_BYTE:
      return processMultiBulkReply(is);
    case COLON_BYTE:
      return processInteger(is);
    case MINUS_BYTE:
      processError(is);
      return null;
    default:
      throw new JedisConnectionException("Unknown reply: " + (char) b);
    }
}
```

- Client To Server

| Protocol Type | First Byte | Example |
|--------|--------|--------|
| Arrays | `*` | `*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` |

```
// redis.clients.jedis.Protocol#sendCommand
public static void sendCommand(final RedisOutputStream os, final ProtocolCommand command,
  final byte[]... args) {
    sendCommand(os, command.getRaw(), args);
}

private static void sendCommand(final RedisOutputStream os, final byte[] command,
  final byte[]... args) {
    try {
      os.write(ASTERISK_BYTE);
      os.writeIntCrLf(args.length + 1);
      os.write(DOLLAR_BYTE);
      os.writeIntCrLf(command.length);
      os.write(command);
      os.writeCrLf();
    
      for (final byte[] arg : args) {
        os.write(DOLLAR_BYTE);
        os.writeIntCrLf(arg.length);
        os.write(arg);
        os.writeCrLf();
      }
    } catch (IOException e) {
      throw new JedisConnectionException(e);
    }
}
```

## Installation
```bash
<dependency>
  <groupId>com.github.myibu</groupId>
  <artifactId>simple-redisclient</artifactId>
  <version>1.0.1a</version>
</dependency>
```

## Examples
```java
RedisClient redisClient = new RedisClient();
RedisClient.RedisReply reply = redisClient.execAndReturn("get foo");
System.out.println(reply);
```
