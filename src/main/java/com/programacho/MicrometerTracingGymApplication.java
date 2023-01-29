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

        StrictCurrentTraceContext traceContext = StrictCurrentTraceContext.create();

        BraveCurrentTraceContext traceContextBridge = new BraveCurrentTraceContext(traceContext);

        Tracing tracing = Tracing.newBuilder()
                .currentTraceContext(traceContext)
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

        brave.Tracer tracer = tracing.tracer();

        Tracer tracerBridge = new BraveTracer(tracer, traceContextBridge, new BraveBaggageManager());

        Random random = new Random();
        server.createContext("/function", exchange -> {
            try (OutputStream os = exchange.getResponseBody()) {
                Span parentSpan = tracerBridge.nextSpan().name("function.parent");
                try (Tracer.SpanInScope parentScope = tracerBridge.withSpan(parentSpan.start())) {
                    tracerBridge.createBaggage("baggage.parent", "value.parent");
                    System.out.println("Baggage in scope: " + tracerBridge.getBaggage("baggage.parent").get());

                    parentSpan.tag("key.parent", "value.parent");
                    parentSpan.event("event.parent1");
                    parentSpan.event("event.parent2");
                    parentSpan.event("event.parent3");

                    Span fooSpan = tracerBridge.nextSpan(parentSpan).name("function.foo");
                    try (Tracer.SpanInScope fooScope = tracerBridge.withSpan(fooSpan.start())) {
                        fooSpan.tag("key.foo", "value.foo");
                        fooSpan.event("event.foo");

                        sleep(random.nextInt(1_000));
                    } finally {
                        fooSpan.end();
                    }

                    Span barSpan = tracerBridge.nextSpan(parentSpan).name("function.bar");
                    try (Tracer.SpanInScope barScope = tracerBridge.withSpan(barSpan.start())) {
                        barSpan.tag("key.bar", "value.bar");
                        barSpan.event("event.bar");

                        sleep(random.nextInt(1_000));

                        throw new RuntimeException("exception.bar");
                    } catch (RuntimeException e) {
                        barSpan.error(e);
                    } finally {
                        barSpan.end();
                    }
                } finally {
                    parentSpan.end();
                }

                System.out.println("Baggage out of scope: " + tracerBridge.getBaggage("baggage.parent").get());
                System.out.println("Context: " + parentSpan.context());

                final byte[] bytes = parentSpan.context().traceId().getBytes(StandardCharsets.UTF_8);
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
