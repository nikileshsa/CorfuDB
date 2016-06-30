package org.corfudb.runtime.view;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.corfudb.infrastructure.*;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.TestClientRouter;
import org.corfudb.runtime.clients.TestClientRule;
import org.junit.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

import static org.corfudb.infrastructure.LayoutServerAssertions.assertThat;
/**
 * Created by mwei on 1/6/16.
 */
public class LayoutViewTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    public void canGetLayout() {
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap(),
                getServerRouterForEndpoint(getDefaultEndpoint())));
        wireRouters();

        CorfuRuntime r = getRuntime().connect();
        Layout l = r.getLayoutView().getCurrentLayout();
        assertThat(l.asJSONString())
                .isNotNull();
    }

    @Test
    public void canSetLayout()
            throws Exception {
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap(),
                getServerRouterForEndpoint(getDefaultEndpoint())));
        wireRouters();

        CorfuRuntime r = getRuntime().connect();
        Layout l = new TestLayoutBuilder()
                        .setEpoch(1L)
                        .addLayoutServer(9000)
                        .addSequencer(9000)
                        .buildSegment()
                            .buildStripe()
                                .addLogUnit(9000)
                                .addToSegment()
                            .addToLayout()
                        .build();

        r.getLayoutView().updateLayout(l, 1L);
        r.invalidateLayout();
        assertThat(r.getLayoutView().getLayout().epoch)
                .isEqualTo(1L);
    }

    @Test
    public void canTolerateLayoutServerFailure()
            throws Exception {
        // No Bootstrap Option Map
        Map<String, Object> noBootstrap = new ImmutableMap.Builder<String,Object>()
                .put("--initial-token", "0")
                .put("--memory", true)
                .put("--single", false)
                .put("--max-cache", "256M")
                .put("--sync", false)
                .build();

        // Server @ 9000 : Layout, Sequencer, LogUnit
        addServerForTest(getDefaultEndpoint(), new LayoutServer(noBootstrap,
                getServerRouterForEndpoint(getDefaultEndpoint())));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));

        // Server @ 9001 : Layout
        LayoutServer failingServer = new LayoutServer(noBootstrap,
                getServerRouterForEndpoint(getEndpoint(9001)));

        addServerForTest(getEndpoint(9001), failingServer);
        wireRouters();

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addLayoutServer(9001)
                .addSequencer(9000)
                    .buildSegment()
                        .buildStripe()
                            .addLogUnit(9000)
                        .addToSegment()
                    .addToLayout()
                .build();

        // Bootstrap with this layout.
        getTestRouterForEndpoint(getEndpoint(9000L)).getClient(LayoutClient.class)
                .bootstrapLayout(l);
        getTestRouterForEndpoint(getEndpoint(9001L)).getClient(LayoutClient.class)
                .bootstrapLayout(l);

        CorfuRuntime r = getRuntime().connect();

        // Fail the network link between the client and test server 9001
        TestClientRouter tcr = getTestRouterForEndpoint(getEndpoint(9001));
        tcr.clientToServerRules.add(new TestClientRule()
                                        .always()
                                        .drop());
        r.invalidateLayout();

        r.getStreamsView().get(CorfuRuntime.getStreamID("hi")).check();
    }

    @Test
    public void layoutPastProposalAdoptedNoQuorum()
            throws Exception {
        // No Bootstrap Option Map
        Map<String, Object> noBootstrap = new ImmutableMap.Builder<String,Object>()
                .put("--initial-token", "0")
                .put("--memory", true)
                .put("--single", false)
                .put("--max-cache", "256M")
                .put("--sync", false)
                .build();

        // Server @ 9000 : Layout, Sequencer, LogUnit
        LayoutServer l9000 = new LayoutServer(noBootstrap,
                getServerRouterForEndpoint(getDefaultEndpoint()));
        addServerForTest(getDefaultEndpoint(), l9000);
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));

        // Server @ 9001 : Layout
        LayoutServer l9001 = new LayoutServer(noBootstrap,
                getServerRouterForEndpoint(getEndpoint(9001)));

        // Server @ 9002 : Layout
        LayoutServer l9002 = new LayoutServer(noBootstrap,
                getServerRouterForEndpoint(getEndpoint(9002)));

        addServerForTest(getEndpoint(9001), l9001);
        addServerForTest(getEndpoint(9002), l9002);

        wireRouters();

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addLayoutServer(9001)
                .addLayoutServer(9002)
                .addSequencer(9000)
                .buildSegment()
                    .buildStripe()
                        .addLogUnit(9000)
                    .addToSegment()
                    .addToLayout()
                .build();

        // Bootstrap with this layout.
        getTestRouterForEndpoint(getEndpoint(9000L)).getClient(LayoutClient.class)
                .bootstrapLayout(l);
        getTestRouterForEndpoint(getEndpoint(9001L)).getClient(LayoutClient.class)
                .bootstrapLayout(l);
        getTestRouterForEndpoint(getEndpoint(9002L)).getClient(LayoutClient.class)
                .bootstrapLayout(l);

        CorfuRuntime r = getRuntime().connect();

        l.setEpoch(2L);

        // Setup the failure scenario: 9001 and 9002 should NOT
        // get the LAYOUT_PROPOSE message.
        // Fail the network link between the client and test server 9001

        getTestRouterForEndpoint(getEndpoint(9001))
                .clientToServerRules.add(new TestClientRule()
                .matches(x -> x.getMsgType().equals(CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE))
                .drop());

        getTestRouterForEndpoint(getEndpoint(9002))
                .clientToServerRules.add(new TestClientRule()
                .matches(x -> x.getMsgType().equals(CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE))
                .drop());

        // Update the layout with our new layout
        r.getLayoutView().updateLayout(l, 0xDEADBEEF);

        // Ensure that the propose only reached l9000
        assertThat(l9000).isInEpoch(2L);
        assertThat(l9001).isInEpoch(1L);
        assertThat(l9002).isInEpoch(1L);

        // Prepare with a even higher rank and use a higher epoch for the layout
        l.getSequencers().add(getEndpoint(9001));
        r.getLayoutView().updateLayout(l, 0xDFFFFFFF);

        // The layout server should NOT adopt the new proposal, and have
        // only one sequencer
        assertThat(l9000).layoutHasSequencerCount(1);
    }
}
