package com.programacho;

import brave.Tracing;
import brave.baggage.BaggageField;
import brave.baggage.BaggagePropagation;
import brave.baggage.BaggagePropagationConfig;
import brave.handler.SpanHandler;
import brave.propagation.StrictCurrentTraceContext;
import brave.sampler.Sampler;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.brave.bridge.BraveBaggageManager;
import io.micrometer.tracing.brave.bridge.BraveCurrentTraceContext;
import io.micrometer.tracing.brave.bridge.BraveTracer;
import io.micrometer.tracing.brave.bridge.W3CPropagation;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.brave.ZipkinSpanHandler;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MicrometerTracingGymApplication {
    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);

        SpanHandler spanHandler = ZipkinSpanHandler.create(AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans")));

        StrictCurrentTraceContext braveContext = StrictCurrentTraceContext.create();

        BraveCurrentTraceContext bridgeContext = new BraveCurrentTraceContext(braveContext);

        Tracing tracing = Tracing.newBuilder()
                .currentTraceContext(braveContext)
                .supportsJoin(false)
                .traceId128Bit(true)
                .localServiceName("micrometer-tracing-gym")
                .propagationFactory(BaggagePropagation.newFactoryBuilder(new W3CPropagation())
                        .add(BaggagePropagationConfig.SingleBaggageField.remote(BaggageField.create("baggage.parent")))
                        .build()
                )
                .sampler(Sampler.ALWAYS_SAMPLE)
                .addSpanHandler(spanHandler)
                .build();

        brave.Tracer braveTracer = tracing.tracer();

        Tracer bridgeTracer = new BraveTracer(braveTracer, bridgeContext, new BraveBaggageManager());

        final Random random = new Random();
        server.createContext("/function", exchange -> {
            try (OutputStream os = exchange.getResponseBody()) {
                Span parent = bridgeTracer.nextSpan().name("function.parent");
                try (Tracer.SpanInScope parentScope = bridgeTracer.withSpan(parent.start())) {
                    bridgeTracer.createBaggage("baggage.parent", "value.parent");
                    System.out.println("Baggage in scope: " + bridgeTracer.getBaggage("baggage.parent").get());

                    parent.tag("key.parent", "value.parent");
                    parent.event("event.parent1");
                    parent.event("event.parent2");
                    parent.event("event.parent3");

                    Span foo = bridgeTracer.nextSpan(parent).name("function.foo");
                    try (Tracer.SpanInScope fooScope = bridgeTracer.withSpan(foo.start())) {
                        foo.tag("key.foo", "value.foo");
                        foo.event("event.foo");

                        sleep(random.nextInt(1_000));
                    } finally {
                        foo.end();
                    }

                    Span bar = bridgeTracer.nextSpan(parent).name("function.bar");
                    try (Tracer.SpanInScope barScope = bridgeTracer.withSpan(bar.start())) {
                        bar.tag("key.bar", "value.bar");
                        bar.event("event.bar");

                        sleep(random.nextInt(1_000));

                        throw new RuntimeException("exception.bar");
                    } catch (RuntimeException e) {
                        bar.error(e);
                    } finally {
                        bar.end();
                    }
                } finally {
                    parent.end();
                }

                System.out.println("Baggage out of scope: " + bridgeTracer.getBaggage("baggage.parent").get());
                System.out.println("Context: " + parent.context());

                final byte[] bytes = parent.context().traceId().getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                os.write(bytes);
            }
        });

        server.start();
    }

    private static void sleep(int timeout) {
        try {
            TimeUnit.MILLISECONDS.sleep(timeout);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
