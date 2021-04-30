package com.example.rsocket;

import com.alibaba.rsocket.RSocketAppContext;
import com.alibaba.rsocket.metadata.*;
import com.alibaba.rsocket.transport.NetworkUtil;
import com.google.common.collect.Lists;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import org.HdrHistogram.Histogram;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;

public class BrokerClient {

    private static final Logger logger =
            LogManager.getLogger(BrokerClient.class);

    public static MessageMimeTypeMetadata jsonMetaEncoding = new MessageMimeTypeMetadata(RSocketMimeType.Json);

    public static void main(String... args) {

        String host = System.getProperty("host", "127.0.0.1");
        int port = Integer.getInteger("port", 8001);
        int count = Integer.getInteger("count", 1000000);
        int concurrency = Integer.getInteger("concurrency", 192);
        int threads = Integer.getInteger("threads", 1);

        Histogram histogram = new Histogram(3600000000000L, 3);


        logger.info("starting test - sending {}", count);
        CountDownLatch latch = new CountDownLatch(threads);
        long start = System.nanoTime();
        Flux.range(1, threads)
                .flatMap(
                        i -> {
                            return Mono.fromRunnable(
                                    () -> {
                                        logger.info("start thread -> {}", i);

                                        RSocket socket =
                                                RSocketConnector.create()
                                                        .setupPayload(BrokerClient.setupPayload().get())
                                                        .metadataMimeType(RSocketMimeType.CompositeMetadata.getType())
                                                        .dataMimeType(RSocketMimeType.Hessian.getType())
                                                        .connect(TcpClientTransport.create("localhost", 9999))
                                                        .block();

                                        int i1 = count / threads;
                                        try {
                                            latch.countDown();
                                            latch.await();
                                        } catch (InterruptedException e) {
                                            throw new RuntimeException(e);
                                        }
                                        Flux.range(0, i1)
                                                .flatMap(
                                                        integer -> {
                                                            GSVRoutingMetadata routingMetadata = new GSVRoutingMetadata("", "com.alibaba.user.UserService", "findById", "");
                                                            RSocketCompositeMetadata compositeMetadata = RSocketCompositeMetadata.from(routingMetadata, BrokerClient.jsonMetaEncoding);


                                                            long s = System.nanoTime();
                                                            return socket.requestResponse(ByteBufPayload.create(EMPTY_BUFFER, compositeMetadata.getContent()))
                                                                    .doFinally(
                                                                            simpleResponse -> {
                                                                                histogram.recordValue(System.nanoTime() - s);
                                                                            });
                                                        },
                                                        concurrency)
                                                .blockLast();
                                    })
                                    .subscribeOn(Schedulers.elastic());
                        })
                .blockLast();


        histogram.outputPercentileDistribution(System.out, 1000.0d);
        double completedMillis = (System.nanoTime() - start) / 1_000_000d;
        double tps = count / (completedMillis / 1_000d);
        logger.info("test complete in {} ms", completedMillis);
        logger.info("test tps {}", tps);
    }

    public static Supplier<Payload> setupPayload() {
        return () -> {
            //composite metadata with app metadata
            RSocketCompositeMetadata compositeMetadata = RSocketCompositeMetadata.from(getAppMetadata());
            return ByteBufPayload.create(Unpooled.EMPTY_BUFFER, compositeMetadata.getContent());
        };
    }

    private static AppMetadata getAppMetadata() {
        //app metadata
        AppMetadata appMetadata = new AppMetadata();
        appMetadata.setUuid(RSocketAppContext.ID);
        appMetadata.setName("broker.vertx.client");
        appMetadata.setIp(NetworkUtil.LOCAL_IP);
        appMetadata.setDevice("SpringBootApp");
        appMetadata.setRsocketPorts(RSocketAppContext.rsocketPorts);
        //brokers
        appMetadata.setBrokers(Lists.newArrayList("tcp://127.0.0.1:9999"));
        appMetadata.setTopology("intranet");
        //web port
        appMetadata.setWebPort(Integer.parseInt("0"));
        appMetadata.setManagementPort(appMetadata.getWebPort());
        if (appMetadata.getWebPort() <= 0) {
            appMetadata.setWebPort(RSocketAppContext.webPort);
        }
        if (appMetadata.getManagementPort() <= 0) {
            appMetadata.setManagementPort(RSocketAppContext.managementPort);
        }
        //labels
        appMetadata.setMetadata(new HashMap<>());
        //power unit
        if (appMetadata.getMetadata("power-rating") != null) {
            appMetadata.setPowerRating(Integer.parseInt(appMetadata.getMetadata("power-rating")));
        }
        return appMetadata;
    }
}
