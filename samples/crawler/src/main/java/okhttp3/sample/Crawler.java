/*
 * Copyright (C) 2014 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package okhttp3.sample;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.Cache;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.OkUrlFactory;
import okhttp3.internal.NamedRunnable;
import okhttp3.internal.Util;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

/**
 * Fetches HTML from a requested URL, follows the links, and repeats.
 */
public final class Crawler {
  private final OkHttpClient client;
  private final Set<HttpUrl> fetchedUrls = Collections.synchronizedSet(
      new LinkedHashSet<HttpUrl>());
  private final LinkedBlockingQueue<HttpUrl> queue = new LinkedBlockingQueue<>();
  private final ConcurrentHashMap<String, AtomicInteger> hostnames = new ConcurrentHashMap<>();

  public Crawler(OkHttpClient client) {
    this.client = client;
  }

  private void parallelDrainQueue(int threadCount) {
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    for (int i = 0; i < threadCount; i++) {
      executor.execute(new NamedRunnable("Crawler %s", i) {
        @Override protected void execute() {
          try {
            drainQueue();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
    }
    executor.shutdown();
  }

  private void drainQueue() throws Exception {
    for (HttpUrl url; (url = queue.take()) != null; ) {
      if (!fetchedUrls.add(url)) {
        continue;
      }

      try {
        fetch(url);
      } catch (IOException e) {
        System.out.printf("XXX: %s %s%n", url, e);
      }
    }
  }

  public void fetch(HttpUrl url) throws IOException {
    // Skip hosts that we've visited many times.
    AtomicInteger hostnameCount = new AtomicInteger();
    AtomicInteger previous = hostnames.putIfAbsent(url.host(), hostnameCount);
    if (previous != null) hostnameCount = previous;
    if (hostnameCount.incrementAndGet() > 100) return;

    HttpURLConnection connection = new OkUrlFactory(client).open(url.url());
    connection.connect();
    try (InputStream in = connection.getInputStream()) {
      int responseCode = connection.getResponseCode();
      String responseSource = "(" + connection.getHeaderField("OkHttp-Response-Source") +
          " over " + connection.getHeaderField("OkHttp-Selected-Protocol") + ")";

      System.out.printf("%03d: %s %s%n", responseCode, url, responseSource);

      String contentType = connection.getHeaderField("Content-Type");
      if (responseCode != 200 || contentType == null) return;

      MediaType mediaType = MediaType.parse(contentType);
      if (mediaType == null || !mediaType.subtype().equals("html")) return;

      Charset charset = mediaType.charset(Util.UTF_8);

      Document document = Jsoup.parse(in, charset.name(), url.toString());
      for (Element element : document.select("a[href]")) {
        String href = element.attr("href");
        HttpUrl link = HttpUrl.get(connection.getURL()).resolve(href);
        if (link != null) queue.add(link);
      }
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.out.println("Usage: Crawler <cache dir> <root>");
      return;
    }

    int threadCount = 20;
    long cacheByteCount = 1024L * 1024L * 100L;

    Cache cache = new Cache(new File(args[0]), cacheByteCount);
    OkHttpClient client = new OkHttpClient.Builder()
        .cache(cache)
        .build();

    Crawler crawler = new Crawler(client);
    crawler.queue.add(HttpUrl.parse(args[1]));
    crawler.parallelDrainQueue(threadCount);
  }
}
