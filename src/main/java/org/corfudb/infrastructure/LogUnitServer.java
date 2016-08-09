package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.corfudb.infrastructure.log.*;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.ReadResultType;
import org.corfudb.util.Utils;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalAndSentinelRetry;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Created by mwei on 12/10/15.
 * <p>
 * A Log Unit Server, which is responsible for providing the persistent storage for the Corfu Distributed Shared Log.
 * <p>
 * All reads and writes go through a cache. If the sync flag (--sync) is set, the cache is configured in write-through
 * mode, otherwise the cache is configured in write-back mode. For persistence, every 10,000 log entries are written
 * to individual files (logs), which are represented as FileHandles. Each FileHandle contains a pointer to the tail
 * of the file, a memory-mapped file channel, and a set of addresses known to be in the file. To write an entry, the
 * pointer to the tail is first extended to the length of the entry, and the entry is added to the set of known
 * addresses. A header is written, which consists of the ASCII characters LE, followed by a set of flags,
 * the log unit address, the size of the entry, then the metadata size, metadata and finally the entry itself.
 * When the entry is complete, a written flag is set in the flags field.
 */
@Slf4j
public class LogUnitServer extends AbstractServer {

    /**
     * A scheduler, which is used to schedule periodic tasks like garbage collection.
     */
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("LogUnit-Maintenance-%d")
                            .build());
    /**
     * The options map.
     */
    Map<String, Object> opts;

    /** Handler for the base server */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
                                            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * Service an incoming write request.
     */
    @ServerHandler(type=CorfuMsgType.WRITE)
    public void write(CorfuPayloadMsg<WriteRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        long address = msg.getPayload().getGlobalAddress();
        log.trace("Write[{}]", address);
        // The payload in the message is a view of a larger buffer allocated
        // by netty, thus direct memory can leak. Copy the view and release the
        // underlying buffer
        LogUnitEntry e = new LogUnitEntry(address, msg.getPayload().getDataBuffer().copy(),
                msg.getPayload().getMetadataMap(), false);
        msg.getPayload().getDataBuffer().release();
        try {
            if (msg.getPayload().getWriteMode() != WriteMode.REPLEX_STREAM) {
                dataCache.put(new LogAddress(e.address, null), e);
            }
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OK.msg());
        } catch (Exception ex) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE.msg());
            e.getBuffer().release();
        }
    }

    @ServerHandler(type=CorfuMsgType.READ_REQUEST)
    private void read_normal(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("Read[{}]", msg.getPayload());
        LogUnitEntry e = dataCache.get(new LogAddress(msg.getPayload(), null));
        if (e == null) {
            r.sendResponse(ctx, msg, new LogUnitReadResponseMsg(ReadResultType.EMPTY));
        } else if (e.isHole) {
            r.sendResponse(ctx, msg, new LogUnitReadResponseMsg(ReadResultType.FILLED_HOLE));
        } else {
            r.sendResponse(ctx, msg, new LogUnitReadResponseMsg(e));
        }
    }

    @ServerHandler(type=CorfuMsgType.READ_RANGE)
    private void read_range(CorfuRangeMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        read(msg, ctx, r);
    }

    @ServerHandler(type=CorfuMsgType.GC_INTERVAL)
    private void gc_interval(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx, IServerRouter r) {
        gcRetry.setRetryInterval(msg.getPayload());
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type=CorfuMsgType.FORCE_GC)
    private void force_gc(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        gcThread.interrupt();
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type=CorfuMsgType.FILL_HOLE)
    private void fill_hole(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx, IServerRouter r) {
        dataCache.get(new LogAddress(msg.getPayload(), null), x -> new LogUnitEntry(msg.getPayload()));
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type=CorfuMsgType.TRIM)
    private void trim(CorfuPayloadMsg<TrimRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        trimMap.compute(msg.getPayload().getStream(), (key, prev) ->
                prev == null ? msg.getPayload().getPrefix() : Math.max(prev, msg.getPayload().getPrefix()));
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }


    /**
     * The garbage collection thread.
     */
    Thread gcThread;

    ConcurrentHashMap<UUID, Long> trimMap;
    IntervalAndSentinelRetry gcRetry;
    AtomicBoolean running = new AtomicBoolean(true);
    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent storage.
     */
    LoadingCache<LogAddress, LogUnitEntry> dataCache;
    long maxCacheSize;

    private final AbstractLocalLog localLog;

    public LogUnitServer(Map<String, Object> opts) {
        this.opts = opts;

        maxCacheSize = Utils.parseLong(opts.get("--max-cache"));
        String logdir = opts.get("--log-path") + File.separator + "log";
        if ((Boolean) opts.get("--memory")) {
            log.warn("Log unit opened in-memory mode (Maximum size={}). " +
                    "This should be run for testing purposes only. " +
                    "If you exceed the maximum size of the unit, old entries will be AUTOMATICALLY trimmed. " +
                    "The unit WILL LOSE ALL DATA if it exits.", Utils.convertToByteStringRepresentation(maxCacheSize));
            localLog = new InMemoryLog(0, Long.MAX_VALUE);
            reset();
        } else {
            localLog = new RollingLog(0, Long.MAX_VALUE, logdir, (Boolean) opts.get("--sync"));
        }

        reset();

/*       compactTail seems to be broken, disabling it for now
         scheduler.scheduleAtFixedRate(this::compactTail,
                Utils.getOption(opts, "--compact", Long.class, 60L),
                Utils.getOption(opts, "--compact", Long.class, 60L),
                TimeUnit.SECONDS);*/

        gcThread = new Thread(this::runGC);
        gcThread.start();
    }

    @Override
    public void reset() {

        if (dataCache != null) {
            /** Free all references */
            dataCache.asMap().values().parallelStream()
                    .map(m -> m.buffer.release());
        }

        dataCache = Caffeine.newBuilder()
                .<LogAddress,LogUnitEntry>weigher((k, v) -> v.buffer == null ? 1 : v.buffer.readableBytes())
                .maximumWeight(maxCacheSize)
                .removalListener(this::handleEviction)
                .writer(new CacheWriter<LogAddress, LogUnitEntry>() {
                    @Override
                    public void write(LogAddress address, LogUnitEntry entry) {
                        if (dataCache.getIfPresent(address) != null) {
                            throw new RuntimeException("overwrite");
                        }
                        if (!entry.isPersisted) { //don't persist an entry twice.
                            localLog.write(address.getAddress(), entry);
                        }
                    }

                    @Override
                    public void delete(LogAddress aLong, LogUnitEntry logUnitEntry, RemovalCause removalCause) {
                        // never need to delete
                    }
                }).build(this::handleRetrieval);

        // Trim map is set to empty on start
        // TODO: persist trim map - this is optional since trim is just a hint.
        trimMap = new ConcurrentHashMap<>();
    }

    /**
     * Retrieve the LogUnitEntry from disk, given an address.
     *
     * @param address The address to retrieve the entry from.
     * @return The log unit entry to retrieve into the cache.
     * This function should not care about trimmed addresses, as that is handled in
     * the read() and write(). Any address that cannot be retrieved should be returned as
     * unwritten (null).
     */
    public synchronized LogUnitEntry handleRetrieval(LogAddress address) {
        LogUnitEntry entry = localLog.read(address.getAddress());
        log.trace("Retrieved[{} : {}]", address, entry);
        return entry;
    }

    public synchronized void handleEviction(LogAddress address, LogUnitEntry entry, RemovalCause cause) {
        log.trace("Eviction[{}]: {}", address, cause);
        if (entry.buffer != null) {
            // Free the internal buffer once the data has been evicted (in the case the server is not sync).
            entry.buffer.release();
        }
    }

    /**
     * Service an incoming ranged read request.
     */
    public void read(CorfuRangeMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("ReadRange[{}]", msg.getRanges());
        Set<LogAddress> total = new HashSet<>();
        for (Range<Long> range : msg.getRanges().asRanges()) {
            total.addAll(Utils.discretizeRange(range).parallelStream()
                            .map(x -> new LogAddress(x, null))
                            .collect(Collectors.toSet()));
        }

        Map<LogAddress, LogUnitEntry> e = dataCache.getAll(total);
        Map<Long, LogUnitReadResponseMsg> o = new ConcurrentHashMap<>();
        e.entrySet().parallelStream()
                .forEach(rv -> o.put(rv.getKey().getAddress(), new LogUnitReadResponseMsg(rv.getValue())));
        r.sendResponse(ctx, msg, new LogUnitReadRangeResponseMsg(o));
    }


    public void runGC() {
        Thread.currentThread().setName("LogUnit-GC");
        val retry = IRetry.build(IntervalAndSentinelRetry.class, this::handleGC)
                .setOptions(x -> x.setSentinelReference(running))
                .setOptions(x -> x.setRetryInterval(60_000));

        gcRetry = (IntervalAndSentinelRetry) retry;

        retry.runForever();
    }

    @SuppressWarnings("unchecked")
    public boolean handleGC() {
        log.info("Garbage collector starting...");
        long freedEntries = 0;

        /* Pick a non-compacted region or just scan the cache */
        Map<LogAddress, LogUnitEntry> map = dataCache.asMap();
        SortedSet<LogAddress> addresses = new TreeSet<>(map.keySet());
        for (LogAddress address : addresses) {
            LogUnitEntry buffer = dataCache.getIfPresent(address);
            if (buffer != null) {
                Set<UUID> streams = buffer.getStreams();
                // this is a normal entry
                if (streams.size() > 0) {
                    boolean trimmable = true;
                    for (java.util.UUID stream : streams) {
                        Long trimMark = trimMap.getOrDefault(stream, null);
                        // if the stream has not been trimmed, or has not been trimmed to this point
                        if (trimMark == null || address.getAddress() > trimMark) {
                            trimmable = false;
                            break;
                        }
                        // it is not trimmable.
                    }
                    if (trimmable) {
                        log.trace("Trimming entry at {}", address);
                        trimEntry(address.getAddress(), streams, buffer);
                        freedEntries++;
                    }
                } else {
                    //this is an entry which belongs in all streams
                }
            }
        }

        log.info("Garbage collection pass complete. Freed {} entries", freedEntries);
        return true;
    }

    public void trimEntry(long address, Set<java.util.UUID> streams, LogUnitEntry entry) {
        // Add this entry to the trimmed range map.
        //trimRange.add(Range.closed(address, address));
        // Invalidate this entry from the cache. This will cause the CacheLoader to free the entry from the disk
        // assuming the entry is back by disk
        dataCache.invalidate(address);
        //and free any references the buffer might have
        if (entry.getBuffer() != null) {
            entry.getBuffer().release();
        }
    }

    /**
     * Shutdown the server.
     */
    @Override
    public void shutdown() {
        scheduler.shutdownNow();
    }

    @VisibleForTesting
    LoadingCache<LogAddress, LogUnitEntry> getDataCache() {
        return dataCache;
    }
}
