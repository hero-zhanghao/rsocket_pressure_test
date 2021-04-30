package com.example.rsocket;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;


public class Server {
    private static final Logger logger = LogManager.getLogger(Server.class);

    public static void main(String[] args) throws InterruptedException {
        String host = System.getProperty("host", "127.0.0.1");
        int port = Integer.getInteger("port", 8001);

        AtomicInteger count = new AtomicInteger();

        RSocket rsocket =
                new RSocket() {
                    @Override
                    public Mono<Payload> requestResponse(Payload p) {
                        count.incrementAndGet();
                        return Mono.just(DefaultPayload.create("hello " + p.getDataUtf8()));
                    }
                };

        RSocketServer.create(SocketAcceptor.with(rsocket))
                .bindNow(TcpServerTransport.create(host, port));

//        RSocket socket =
//                RSocketConnector.connectWith(TcpClientTransport.create(host, port)).block();
//
//        for (int i = 0; i < 3; i++) {
//            socket
//                    .requestResponse(DefaultPayload.create("Hero"))
//                    .map(Payload::getDataUtf8)
//                    .onErrorReturn("error")
//                    .doOnNext(logger::info)
//                    .block();
//        }

//        socket.dispose();

        Mono.delay(Duration.ofSeconds(1))
                .doOnNext(ignore -> logger.info("curr count : {}" , count.get()))
                .repeat()
                .subscribe();

        Thread.sleep(100000000000L);
    }
}
