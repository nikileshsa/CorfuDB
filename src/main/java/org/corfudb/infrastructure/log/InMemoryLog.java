package org.corfudb.infrastructure.log;

import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by maithem on 7/21/16.
 */
public class InMemoryLog extends AbstractLocalLog {

    private final Map<Long, LogUnitEntry> cache;

    public InMemoryLog(long start, long end) {
        super(start, end, "", true);
        cache = new HashMap();
    }

    protected void backendWrite(long address, LogUnitEntry entry) {
        cache.put(address, entry);
    }

    protected LogUnitEntry backendRead(long address) {
        return cache.get(address);
    }

    protected void initializeLog() {
        // no-op
    }

    protected void backendStreamWrite(UUID streamID, RangeSet<Long> entry){
        // no-op
    }

    protected RangeSet<Long> backendStreamRead(UUID streamID) {
        return TreeRangeSet.create();
    }
}
