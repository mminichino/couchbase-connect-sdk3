package com.codelry.util.cbdb3;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.SecurityConfig;
import com.couchbase.client.dcp.highlevel.internal.CollectionIdAndKey;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPOutputStream;

import static java.util.Objects.requireNonNull;

/**
 * Couchbase Stream Utility.
 */
public class CouchbaseStream {
  static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseStream.class);
  private final String hostname;
  private final String username;
  private final String password;
  private final String bucket;
  private final String scope;
  private final String collection;
  private final Boolean useSsl;
  private boolean collectionEnabled;
  private final AtomicLong totalSize = new AtomicLong(0);
  private final AtomicLong docCount = new AtomicLong(0);
  private final AtomicLong sentCount = new AtomicLong(0);
  private final PriorityBlockingQueue<String> queue = new PriorityBlockingQueue<>();
  private Client client;

  public CouchbaseStream(String hostname, String username, String password, String bucket, Boolean ssl) {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.bucket = bucket;
    this.useSsl = ssl;
    this.scope = "_default";
    this.collection = "_default";
    this.init();
  }

  public CouchbaseStream(String hostname, String username, String password, String bucket, Boolean ssl,
                         String scope, String collection) {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.bucket = bucket;
    this.useSsl = ssl;
    this.scope = scope;
    this.collection = collection;
    this.init();
  }

  public void init() {
    StringBuilder connectBuilder = new StringBuilder();

    String couchbasePrefix;
    if (useSsl) {
      couchbasePrefix = "couchbases://";
    } else {
      couchbasePrefix = "couchbase://";
    }

    connectBuilder.append(couchbasePrefix);
    connectBuilder.append(hostname);

    String connectString = connectBuilder.toString();

    collectionEnabled = !collection.equals("_default");

    Consumer<SecurityConfig.Builder> secClientConfig = securityConfig -> {
      securityConfig.enableTls(useSsl)
          .enableHostnameVerification(false)
          .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
    };

    Client.Builder clientBuilder = Client.builder()
        .connectionString(connectString)
        .bucket(bucket)
        .securityConfig(secClientConfig)
        .credentials(username, password);

    if (!scope.equals("_default") || !collection.equals("_default")) {
      clientBuilder.collectionsAware(true).collectionNames(scope + "." + collection);
    }

    client = clientBuilder.build();

    client.controlEventHandler((flowController, event) -> {
      flowController.ack(event);
      event.release();
    });
  }

  public void toCompressedFile(String filename) throws IOException {
    try (FileOutputStream output = new FileOutputStream(filename)) {
      Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), StandardCharsets.UTF_8);
      streamDocuments();
      startToNow();
      streamData().forEach(record -> {
        try {
          writer.write(record + "\n");
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      stop();
      writer.close();
    } catch (Exception e) {
      throw new IOException("Can not stream to file " + filename, e);
    }
  }

  public void toWriter(Writer writer) {
    streamDocuments();
    startToNow();
    streamData().forEach(record -> {
      try {
        writer.write(record + "\n");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    stop();
  }

  public void streamDocuments() {
    client.dataEventHandler((flowController, event) -> {
      if (DcpMutationMessage.is(event)) {
        CollectionIdAndKey key = MessageUtil.getCollectionIdAndKey(event, collectionEnabled);
        byte[] content = DcpMutationMessage.contentBytes(event);
        try {
          ObjectMapper mapper = new ObjectMapper();
          JsonNode document = mapper.readTree(content);
          ObjectNode metadata = mapper.createObjectNode();
          metadata.put("id", key.key());
          ObjectNode root = mapper.createObjectNode();
          root.set("metadata", metadata);
          root.set("document", document);
          queue.add(root.toString());
          flowController.ack(event);
          docCount.incrementAndGet();
        } catch (Exception e) {
          LOGGER.error("Error reading stream: {}", e.getMessage(), e);
        }
      }
      event.release();
    });
  }

  public void startToNow() {
    client.connect().block();
    client.initializeState(StreamFrom.BEGINNING, StreamTo.NOW).block();
    client.startStreaming().block();
  }

  public void startFromNow() {
    client.connect().block();
    client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).block();
    client.startStreaming().block();
  }

  public <T> Stream<T> whileNotNull(Supplier<? extends T> supplier) {
    requireNonNull(supplier);
    return StreamSupport.stream(
        new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.NONNULL) {
          @Override
          public boolean tryAdvance(Consumer<? super T> action) {
            do {
              T element = supplier.get();
              if (element != null) {
                action.accept(element);
                sentCount.incrementAndGet();
                return true;
              }
            } while (!client.sessionState().isAtEnd() || sentCount.get() < docCount.get());
            return false;
          }
        }, false);
  }

  public Stream<String> streamData() {
    try {
      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
    } catch (InterruptedException e) {
      LOGGER.debug("streamData wait interrupted");
    }
    return whileNotNull(queue::poll);
  }

  public Stream<String> getByCount(long count) {
    return Stream.generate(() -> {
          try {
            return queue.take();
          } catch (InterruptedException ex) {
            return null;
          }
        })
        .limit(count);
  }

  public void stop() {
    client.disconnect().block();
  }

  public long getCount() {
    return docCount.get();
  }
}
