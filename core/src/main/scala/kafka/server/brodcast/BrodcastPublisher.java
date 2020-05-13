/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.brodcast;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ConcurrentPublication;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicLong;
import kafka.network.RequestChannel;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * User: fengHong Date: 2020-04-30 16:56
 */
public class BrodcastPublisher {

    private static int termLength = 1024 * 1024;
    private static int brodcastPort = 12306;
    private static final int STREAM_ID = 12306;


    private static String MASTER_IP_STR = getMasterIpStr("en0") != null ? getMasterIpStr("en0") : getMasterIpStr("eth0");
    private static String SRC_LIVE_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .controlEndpoint(MASTER_IP_STR + ":" + brodcastPort)
        .termLength(termLength)
        .build();

    public static final MediaDriver.Context ctxMeiaDriver = new MediaDriver.Context()
        .termBufferSparseFile(false)
        .useWindowsHighResTimer(true)
        .rejoinStream(true)
        .threadingMode(ThreadingMode.SHARED_NETWORK)
        .conductorIdleStrategy(BusySpinIdleStrategy.INSTANCE)
        .receiverIdleStrategy(NoOpIdleStrategy.INSTANCE)
        .senderIdleStrategy(NoOpIdleStrategy.INSTANCE);

    private static MediaDriver mediaDriver = MediaDriver.launch(ctxMeiaDriver);
    private static Aeron aeron;
    private static Aeron.Context ctx;
    private static ConcurrentPublication publication;
    private static final BrodcastPublisher BRODCAST_PUBLISHER = new BrodcastPublisher();

    public static BrodcastPublisher getInstance() {
        return BRODCAST_PUBLISHER;
    }

    private BrodcastPublisher() {
        System.out.println("fh~~~~~~~~~~~~~ brodcast IP= " + MASTER_IP_STR);

        ctx = new Aeron.Context();
        aeron = Aeron.connect(ctx);
        publication = aeron.addPublication(SRC_LIVE_CHANNEL, STREAM_ID);
        System.out.println("fh~~~~~~~~~~~~~ ctxMeiaDriver = " + ctxMeiaDriver);
    }

    private static final ByteBuffer BYTE_BUFFER = ByteBuffer.allocate(200);
    static AtomicLong count = new AtomicLong();

    //fhtodo 111
    public void brodcastMessage(RequestChannel.Request request) {
//    publication.offer(new UnsafeBuffer(request.readOnlyBuffer()));
        BYTE_BUFFER.clear();
        BYTE_BUFFER.putLong(count.incrementAndGet());
        BYTE_BUFFER.putLong(System.currentTimeMillis());
        final long result = publication.offer(new UnsafeBuffer(BYTE_BUFFER));
        if (result < 0L) {
            if (result == Publication.BACK_PRESSURED) {
                System.out.println("Offer failed due to back pressure");
            } else if (result == Publication.NOT_CONNECTED) {
                System.out.println("Offer failed because publisher is not connected to subscriber");
            } else if (result == Publication.ADMIN_ACTION) {
                System.out.println("Offer failed because of an administration action in the system");
            } else if (result == Publication.CLOSED) {
                System.out.println("Offer failed publication is closed");
            } else if (result == Publication.MAX_POSITION_EXCEEDED) {
                System.out.println("Offer failed due to publication reaching max position");
            } else {
                System.out.println("Offer failed due to unknown reason: " + result);
            }
        }
//    publication.offer(new UnsafeBuffer(request.readOnlyBuffer()));

    }

    private static String getMasterIpStr(String nifName) {
        try {
            Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
            while (nifs.hasMoreElements()) {
                NetworkInterface nif = nifs.nextElement();
                Enumeration<InetAddress> addresses = nif.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();

                    if (nif.getName().equals(nifName) && addr instanceof Inet4Address) {
                        return addr.getHostAddress();
                    }
                }
            }
        } catch (SocketException e) {
            return null;
        }
        return null;
    }

    public MediaDriver getMediaDriver() {
        return mediaDriver;
    }

    public static void main(String[] args) {
        System.out.println("getMasterIpStr(\"en0\") = " + getMasterIpStr("en0"));
    }
}
