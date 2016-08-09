package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.SequencerServerAssertions.assertThat;

/**
 * Created by mwei on 12/13/15.
 */
public class SequencerServerTest extends AbstractServerTest {

    public SequencerServerTest() {
        super();
    }

    @Override
    public AbstractServer getDefaultServer() {
        return new
                SequencerServer(new ServerConfigBuilder().build());
    }

    @Test
    public void responseForEachRequest() {
        for (int i = 0; i < 100; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ, new TokenRequest(1L, Collections.<UUID>emptySet())));
            assertThat(getResponseMessages().size())
                    .isEqualTo(i + 1);
        }
    }

    @Test
    public void tokensAreIncreasing() {
        long lastToken = -1;
        for (int i = 0; i < 100; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ, new TokenRequest(1L, Collections.<UUID>emptySet())));
            long thisToken = getLastPayloadMessageAs(TokenResponse.class).getToken();
            assertThat(thisToken)
                    .isGreaterThan(lastToken);
            lastToken = thisToken;
        }
    }

    @Test
    public void checkTokenPositionWorks() {
        for (int i = 0; i < 100; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ, new TokenRequest(1L, Collections.<UUID>emptySet())));
            long thisToken = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.<UUID>emptySet())));
            long checkToken = getLastPayloadMessageAs(TokenResponse.class).getToken();

            assertThat(thisToken)
                    .isEqualTo(checkToken);
        }
    }

    @Test
    public void perStreamCheckTokenPositionWorks() {
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());

        for (int i = 0; i < 100; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamA))));
            long thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singleton(streamA))));
            long checkTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            assertThat(thisTokenA)
                    .isEqualTo(checkTokenA);

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamB))));
            long thisTokenB = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singleton(streamB))));
            long checkTokenB = getLastPayloadMessageAs(TokenResponse.class).getToken();

            assertThat(thisTokenB)
                    .isEqualTo(checkTokenB);

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singleton(streamA))));
            long checkTokenA2 = getLastPayloadMessageAs(TokenResponse.class).getToken();

            assertThat(checkTokenA2)
                    .isEqualTo(checkTokenA);

            assertThat(thisTokenB)
                    .isGreaterThan(checkTokenA2);
        }
    }

    @Test
    public void checkSequencerCheckpointingWorks()
            throws Exception {
        String serviceDir = getTempDir();

        SequencerServer s1 = new SequencerServer(new ServerConfigBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .setInitialToken(0)
                .setCheckpoint(1)
                .build());

        this.router.reset();
        this.router.addServer(s1);
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singleton(CorfuRuntime.getStreamID("a")))));
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singleton(CorfuRuntime.getStreamID("a")))));
        assertThat(s1)
                .tokenIsAt(2);
        Thread.sleep(1400);
        s1.shutdown();

        SequencerServer s2 = new SequencerServer(new ServerConfigBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .setInitialToken(-1)
                .setCheckpoint(1)
                .build());
        this.router.reset();
        this.router.addServer(s2);
        assertThat(s2)
                .tokenIsAt(2);
    }

}
