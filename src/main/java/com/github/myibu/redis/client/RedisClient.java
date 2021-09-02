package com.github.myibu.redis.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Redis客户端
 * @author myibu
 * Create on 2021/8/31
 */
public class RedisClient {
    /**
     * Redis命令
     */
    public enum RedisCommand {
        MODULE, GET, GETEX, GETDEL, SET, SETNX, SETEX, PSETEX, APPEND, STRLEN, DEL, UNLINK, EXISTS, SETBIT,
        GETBIT, BITFIELD, BITFIELD_RO, SETRANGE, GETRANGE, SUBSTR, INCR, DECR, MGET, RPUSH, LPUSH, RPUSHX,
        LPUSHX, LINSERT, RPOP, LPOP, BRPOP, BRPOPLPUSH, BLMOVE, BLPOP, LLEN, LINDEX, LSET, LRANGE, LTRIM,
        LPOS, LREM, RPOPLPUSH, LMOVE, SADD, SREM, SMOVE, SISMEMBER, SMISMEMBER, SCARD, SPOP, SRANDMEMBER,
        SINTER, SINTERSTORE, SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE, SMEMBERS, SSCAN, ZADD, ZINCRBY, ZREM,
        ZREMRANGEBYSCORE, ZREMRANGEBYRANK, ZREMRANGEBYLEX, ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE, ZUNION,
        ZINTER, ZDIFF, ZRANGE, ZRANGESTORE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX, ZREVRANGEBYLEX,
        ZCOUNT, ZLEXCOUNT, ZREVRANGE, ZCARD, ZSCORE, ZMSCORE, ZRANK, ZREVRANK, ZSCAN, ZPOPMIN, ZPOPMAX,
        BZPOPMIN, BZPOPMAX, ZRANDMEMBER, HSET, HSETNX, HGET, HMSET, HMGET, HINCRBY, HINCRBYFLOAT, HDEL,
        HLEN, HSTRLEN, HKEYS, HVALS, HGETALL, HEXISTS, HRANDFIELD, HSCAN, INCRBY, DECRBY, INCRBYFLOAT,
        GETSET, MSET, MSETNX, RANDOMKEY, SELECT, SWAPDB, MOVE, COPY, RENAME, RENAMENX, EXPIRE, EXPIREAT,
        PEXPIRE, PEXPIREAT, KEYS, SCAN, DBSIZE, AUTH, PING, ECHO, SAVE, BGSAVE, BGREWRITEAOF, SHUTDOWN,
        LASTSAVE, TYPE, MULTI, EXEC, DISCARD, SYNC, PSYNC, REPLCONF, FLUSHDB, FLUSHALL, SORT, INFO, MONITOR,
        TTL, TOUCH, PTTL, PERSIST, SLAVEOF, REPLICAOF, ROLE, DEBUG, CONFIG, SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE,
        PUNSUBSCRIBE, PUBLISH, PUBSUB, WATCH, UNWATCH, CLUSTER, RESTORE,  MIGRATE, ASKING, READONLY, READWRITE,
        DUMP, OBJECT, MEMORY, CLIENT, HELLO, EVAL, EVALSHA, SLOWLOG, SCRIPT, TIME, BITOP, BITCOUNT, BITPOS, WAIT,
        COMMAND, GEOADD, GEORADIUS, GEORADIUS_RO, GEORADIUSBYMEMBER, GEORADIUSBYMEMBER_RO, GEOHASH, GEOPOS, GEODIST,
        GEOSEARCH, GEOSEARCHSTORE, PFSELFTEST, PFADD, PFCOUNT, PFMERGE, PFDEBUG, XADD, XRANGE, XREVRANGE, XLEN, XREAD,
        XREADGROUP, XGROUP, XSETID, XACK, XPENDING, XCLAIM, XAUTOCLAIM, XINFO, XDEL, XTRIM, POST, LATENCY, LOLWUT, ACL,
        STRALGO, RESET, FAILOVER;
    }
    /**
     * Redis响应
     */
    public static class RedisReply {
        private RedisReplyType type;
        private Object data;
        private byte[] raw;

        public RedisReplyType getType() {
            return type;
        }

        public void setType(RedisReplyType type) {
            this.type = type;
        }

        public Object getData() {
            return data;
        }

        public void setData(Object data) {
            this.data = data;
        }

        public byte[] getRaw() {
            return raw;
        }

        public void setRaw(byte[] raw) {
            this.raw = raw;
        }

        public static RedisReply create(byte[] buf, int pos, int len, RedisReplyType type) {
            RedisReply reply = new RedisReply();
            byte[] rawBytes = new byte[len];
            System.arraycopy(buf, pos, rawBytes, 0, len);
            reply.setRaw(rawBytes);
            reply.setType(type);
            return reply;
        }
        @Override
        public String toString() {
            String rawString = new String(raw);
            rawString = rawString.replaceAll("\r", "\\\\r");
            rawString = rawString.replaceAll("\n", "\\\\n");
            return "RedisReply{" +
                    "type=" + type +
                    ", data=" + data +
                    ", raw=" + rawString +
                    '}';
        }
    }

    /**
     * Redis服务器响应客户端的类型
     */
    public enum RedisReplyType {
        /**
         * Simple Strings: +OK\r\n
         */
        SIMPLE_STRINGS(PLUS_BYTE),
        /**
         * Errors:  -ERR unknown command 'hello'
         */
        ERRORS(MINUS_BYTE),
        /**
         * Integers:  :2
         */
        INTEGERS(COLON_BYTE),
        /**
         * Bulk Strings:  $6\r\nfoobar\r\n
         */
        BULK_STRINGS(DOLLAR_BYTE),
        /**
         * Arrays:  *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
         */
        ARRAYS(ASTERISK_BYTE);

        final byte first;
        RedisReplyType(byte first) {
            this.first = first;
        }
    }

    public static final byte PLUS_BYTE = '+';
    public static final byte MINUS_BYTE = '-';
    public static final byte COLON_BYTE = ':';
    public static final byte DOLLAR_BYTE = '$';
    public static final byte ASTERISK_BYTE = '*';

    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final String CRLF = "\r\n";
    public static final int MAX_SIZE = 8192;

    private Socket socket;
    private final InetSocketAddress address;
    private RedisOutputStream ros;
    private RedisInputStream ris;

    public RedisClient(){
        this("127.0.0.1", 6379);
    }

    public RedisClient(String hostname, int port) {
        this(new InetSocketAddress(hostname, port));
    }

    public RedisClient(InetSocketAddress socketAddress) {
        address = socketAddress;
        try {
            socket = createSocket(socketAddress);
            ros = new RedisOutputStream(socket.getOutputStream());
            ris = new RedisInputStream(socket.getInputStream());
        } catch (IOException e) {
            throw new RedisException("Create redis client failed.", e);
        }
    }

    private Socket createSocket(InetSocketAddress socketAddress) throws IOException {
        Socket socket = new Socket();
        socket.setReuseAddress(true);
        socket.setKeepAlive(true);
        socket.setTcpNoDelay(true);
        socket.setSoLinger(true, 0);

        socket.connect(socketAddress);
        return socket;
    }

    private void checkConnectState() throws IOException {
        if (socket == null || socket.isClosed() || !socket.isConnected()) {
            socket = createSocket(address);
            ros = new RedisOutputStream(socket.getOutputStream());
            ris = new RedisInputStream(socket.getInputStream());
        }
    }

    /**
     * 执行命令并返回结果
     * @param sentence 命令句子，如：set foo bar
     * @return 执行结果
     */
    public RedisReply execAndReturn(String sentence) {
        String[] args = sentence.split(" ");
        String command = args[0];
        List<String> strings = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            if (!"".equals(args[i])) {
                strings.add(args[i]);
            }
        }
        execCommand(command, strings.toArray(new String[0]));
        try {
            return ris.readReply();
        } catch (IOException e) {
            throw new RedisException("Execute command failed.", e);
        }
    }

    /**
     * 执行命令并返回结果
     * @param command redis命令，如：set
     * @param args redis命令参数： 如： [foo bar]
     * @return 执行结果
     */
    public RedisReply execAndReturn(RedisCommand command, String ...args) {
        execCommand(command.name(), args);
        try {
            return ris.readReply();
        } catch (IOException e) {
            throw new RedisException("Execute command failed.", e);
        }
    }

    /**
     * 执行命令
     * @param sentence 命令句子，如：set foo bar
     */
    public void exec(String sentence) {
        String[] args = sentence.split(" ");
        String command = args[0];
        List<String> strings = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            if (!"".equals(args[i])) {
                strings.add(args[i]);
            }
        }
        try {
            execCommand(command, strings.toArray(new String[0]));
            ris.readReply();
        } catch (IOException e) {
            throw new RedisException("Execute command failed.", e);
        }
    }

    /**
     * 执行命令
     * @param command redis命令，如：set
     * @param args redis命令参数： 如： [foo bar]
     */
    public void exec(RedisCommand command, String ...args) {
        try {
            execCommand(command.name(), args);
            ris.readReply();
        } catch (IOException e) {
            throw new RedisException("Execute command failed.", e);
        }
    }

    private void execCommand(String command, String ...args) {
        try {
            byte[] commandBytes = command.getBytes(DEFAULT_CHARSET);
            byte[][] argBytes = new byte[args.length][];
            for (int i = 0; i < args.length; i++) {
                argBytes[i] = args[i].getBytes(DEFAULT_CHARSET);
            }
            doExecCommand(commandBytes, argBytes);
        } catch (IOException e) {
            throw new RedisException("Execute command failed.", e);
        }
    }

    private void doExecCommand(byte[] command, byte[][] args) {
        try {
            ros.reset();
            checkConnectState();
            ros.write(ASTERISK_BYTE);
            ros.writeIntCrLf(args.length + 1);
            ros.write(DOLLAR_BYTE);
            ros.writeIntCrLf(command.length);
            ros.write(command);
            ros.writeCrLf();

            for (final byte[] arg : args) {
                ros.write(DOLLAR_BYTE);
                ros.writeIntCrLf(arg.length);
                ros.write(arg);
                ros.writeCrLf();
            }
            ros.flush();
        } catch (IOException e) {
            throw new RedisException("Execute command failed.", e);
        }
    }

    public void close() {
        try {
            if (socket != null) {
                socket.close();
            }
            if (ros != null) {
                ros.close();
            }
            if (ris != null) {
                ris.close();
            }
        } catch (IOException e) {
            throw new RedisException("Close client error.", e);
        }
    }
    /**
     * Redis输出流，用于客户端写入数据到服务端
     */
    public static class RedisOutputStream extends OutputStream {
        protected OutputStream fos;
        protected byte[] buf;
        protected int count = 0;

        public RedisOutputStream(OutputStream fos) {
            this(fos, MAX_SIZE);
        }

        public RedisOutputStream(OutputStream fos, int size) {
            this.fos = fos;
            this.buf = new byte[size];
        }

        public synchronized void write(byte b) throws IOException {
            if (count + 1 >= buf.length) {
                fos.write(buf, 0, count);
                fos.flush();
                count = 0;
            }
            buf[count++] = b;
        }

        @Override
        public synchronized void write(final byte[] b, final int off, final int len) throws IOException {
            if (len >= buf.length) {
                flush();
                fos.write(b, off, len);
            } else {
                if (len >= buf.length - count) {
                    flush();
                }
                System.arraycopy(b, off, buf, count, len);
                count += len;
            }
        }

        @Override
        public synchronized void write(int b) throws IOException {
            byte[] bytes = Integer.valueOf(b).toString().getBytes(DEFAULT_CHARSET);
            if (count + bytes.length >= buf.length) {
                flush();
            }
            System.arraycopy(bytes, 0, buf, count, bytes.length);
            count += bytes.length;
        }

        public synchronized void writeCrLf() throws IOException {
            if (count + CRLF.length() >= buf.length) {
                flush();
            }
            buf[count++] = '\r';
            buf[count++] = '\n';
        }

        public synchronized void writeIntCrLf(int value) throws IOException {
            write(value);
            writeCrLf();
        }

        @Override
        public void flush() throws IOException {
            fos.write(buf, 0, count);
            super.flush();
            count = 0;
        }

        public void reset() {
            count = 0;
        }
    }

    /**
     * Redis输入流，用于客户端从服务端读取数据
     */
    public static class RedisInputStream extends InputStream {
        protected InputStream fis;
        protected byte[] buf;
        protected int count = 0, pos = 0;
        private final ByteArrayOutputStream bos;

        public RedisInputStream(InputStream fis) {
            this(fis, MAX_SIZE);
        }

        public RedisInputStream(InputStream fis, int size) {
            this.fis = fis;
            this.buf = new byte[size];
            this.bos = new ByteArrayOutputStream();
        }

        public RedisReply readReply() throws IOException {
            count = 0;
            pos = 0;
            byte firstByte = (byte) read();
            RedisReply reply;
            switch (firstByte) {
                case PLUS_BYTE:
                    reply = readSimpleStringsReply();
                    break;
                case MINUS_BYTE:
                    reply = readErrorsReply();
                    break;
                case COLON_BYTE:
                    reply = readIntegersReply();
                    break;
                case DOLLAR_BYTE:
                    reply = readBulkStringsReply();
                    break;
                case ASTERISK_BYTE:
                    reply = readArraysReply();
                    break;
                default:
                    throw new RedisException("Unknown protocol begin with byte " + firstByte);
            }
            return reply;
        }

        private String readStringCrlf() {
            bos.reset();
            int pre = pos;
            while (pos < count) {
                if (buf[pos++] == '\r') {
                    if (buf[pos] == '\n') {
                        break;
                    }
                }
            }
            bos.write(buf, pre, pos-pre-1);
            return new String(bos.toByteArray());
        }

        private int readIntegerCrlf() {
            int value = 0;
            boolean isNegative = false;
            if (buf[pos] == '-') {
                pos++;
                isNegative = true;
            }
            while (pos < count) {
                final int b = buf[pos++];
                if (b == '\r') {
                    if (buf[pos++] == '\n') {
                        break;
                    }
                } else {
                    value = value * 10 + b - '0';
                }
            }
            return isNegative ? -value : value;
        }


        /**
         * Simple Strings:  +OK\r\n
         * @return RedisReply
         */
        private RedisReply readSimpleStringsReply() {
            RedisReply reply = RedisReply.create(buf, pos-1, count, RedisReplyType.SIMPLE_STRINGS);
            reply.setData(readStringCrlf());
            return reply;
        }

        /**
         * Errors:  -ERR unknown command 'hello'
         * @return RedisReply
         */
        private RedisReply readErrorsReply() {
            RedisReply reply = RedisReply.create(buf, pos-1, count, RedisReplyType.ERRORS);
            // skip "ERR"
            pos += 4;
            reply.setData(readStringCrlf());
            return reply;
        }

        /**
         * Integers:  :2
         * @return RedisReply
         */
        private RedisReply readIntegersReply() {
            RedisReply reply = RedisReply.create(buf, pos-1, count, RedisReplyType.INTEGERS);
            reply.setData(Integer.valueOf(readStringCrlf()));
            return reply;
        }

        /**
         * Bulk Strings:  $6\r\nfoobar\r\n
         * @return RedisReply
         */
        private RedisReply readBulkStringsReply() {
            int pre = pos-1;
            int value = readIntegerCrlf();
            bos.reset();
            bos.write(buf, pos, value);
            pos = pos + value + 2;
            RedisReply reply = RedisReply.create(buf, pre, pos - pre, RedisReplyType.BULK_STRINGS);
            reply.setData(new String(bos.toByteArray()));
            return reply;
        }

        /**
         * Arrays:  *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
         * @return RedisReply
         */
        private RedisReply readArraysReply() {
            RedisReply reply = RedisReply.create(buf, pos-1, count, RedisReplyType.ARRAYS);
            int value = readIntegerCrlf();
            List<RedisReply> replies = new ArrayList<>(value);
            for (int i = 0; i < value; i++) {
                byte firstByte = buf[pos++];
                RedisReply innerReply;
                switch (firstByte) {
                    case COLON_BYTE:
                        innerReply = readIntegersReply();
                        break;
                    case DOLLAR_BYTE:
                        innerReply = readBulkStringsReply();
                        break;
                    default:
                        throw new RedisException("Unknown protocol begin with byte " + firstByte);
                }
                replies.add(innerReply);
            }
            reply.setData(replies);
            return reply;
        }

        @Override
        public int read() throws IOException {
            int len = fis.read(buf);
            count += len;
            return buf[pos++];
        }
    }

    /**
     * Redis异常
     */
    public static class RedisException extends RuntimeException {
        public RedisException() {
            super();
        }

        public RedisException(String s) {
            super(s);
        }

        public RedisException(String message, Throwable cause) {
            super(message, cause);
        }

        public RedisException(Throwable cause) {
            super(cause);
        }
    }
}
