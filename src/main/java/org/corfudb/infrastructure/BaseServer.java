package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.util.CorfuMsgHandler;
import org.corfudb.util.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class BaseServer extends AbstractServer {

    /** Options map, if available */
    @Getter
    @Setter
    public Map<String, Object> optionsMap = new HashMap<>();

    /** Handler for the base server */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .addHandler(CorfuMsg.CorfuMsgType.PING, BaseServer::ping)
            .addHandler(CorfuMsg.CorfuMsgType.SET_EPOCH, BaseServer::setEpoch)
            .addHandler(CorfuMsg.CorfuMsgType.RESET, BaseServer::doReset)
            .addHandler(CorfuMsg.CorfuMsgType.VERSION_REQUEST, this::getVersion);

    /** Respond to a ping message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    private static void ping(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.PONG));
    }

    /** Respond to a epoch change message.
     *
     * @param csem      The incoming message
     * @param ctx       The channel context
     * @param r         The server router.
     */
    private static void setEpoch(CorfuPayloadMsg<Long> csem, ChannelHandlerContext ctx, IServerRouter r) {
        if (csem.getPayload() >= r.getServerEpoch()) {
            log.info("Received SET_EPOCH, moving to new epoch {}", csem.getPayload());
            r.setServerEpoch(csem.getPayload());
            r.sendResponse(ctx, csem, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
        } else {
            log.debug("Rejected SET_EPOCH currrent={}, requested={}",
                    r.getServerEpoch(), csem.getPayload());
            r.sendResponse(ctx, csem, new CorfuPayloadMsg<>(CorfuMsg.CorfuMsgType.WRONG_EPOCH,
                    r.getServerEpoch()));
        }
    }

    /** Respond to a version request message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    private void getVersion(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        VersionInfo vi = new VersionInfo(optionsMap);
        r.sendResponse(ctx, msg, new JSONPayloadMsg<>(vi, CorfuMsg.CorfuMsgType.VERSION_RESPONSE));
    }

    /** Reset the JVM.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    private static void doReset(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.warn("Remote reset requested from client " + msg.getClientID());
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
        Utils.sleepUninterruptibly(500);
        System.exit(100);
    }

    @Override
    public void reset() {

    }
}
