import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Multi-threaded Web Crawler
 * ---------------------------
 * - Uses a thread pool (ExecutorService) to crawl pages concurrently
 * - Uses a ConcurrentHashMap-backed Set to avoid duplicate visits
 * - Uses an AtomicInteger to track active tasks so the pool knows when to stop
 * - Builds a simple inverted index (word -> set of URLs) for basic search
 *
 * Compile: javac MultiThreadedWebCrawler.java
 * Run:     java MultiThreadedWebCrawler
 */
public class MultiThreadedWebCrawler {

    // ---- Configuration ----
    private static final int THREAD_POOL_SIZE = 8;
    private static final int MAX_PAGES_TO_CRAWL = 30;
    private static final int MAX_DEPTH = 2;

    // ---- Shared thread-safe state ----
    private final Set<String> visitedUrls = ConcurrentHashMap.newKeySet();
    private final Map<String, Set<String>> invertedIndex = new ConcurrentHashMap<>();
    private final AtomicInteger pagesCrawled = new AtomicInteger(0);
    private final AtomicInteger activeTasks = new AtomicInteger(0);

    private final ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(java.time.Duration.ofSeconds(5))
            .build();

    // Regex for extracting href links from raw HTML (simple, no external HTML parser needed)
    private static final Pattern LINK_PATTERN = Pattern.compile(
            "href=[\"']?((https?://)[^\"'>\\s]+)", Pattern.CASE_INSENSITIVE);

    public static void main(String[] args) {
        // Seed URLs to start crawling from
        List<String> seedUrls = List.of(
                "https://example.com",
                "https://www.wikipedia.org"
        );

        MultiThreadedWebCrawler crawler = new MultiThreadedWebCrawler();
        crawler.startCrawl(seedUrls);
    }

    public void startCrawl(List<String> seedUrls) {
        System.out.println("=== Multi-threaded Web Crawler Starting ===");
        System.out.println("Thread pool size: " + THREAD_POOL_SIZE);
        System.out.println("Max pages: " + MAX_PAGES_TO_CRAWL + " | Max depth: " + MAX_DEPTH);
        System.out.println("=============================================\n");

        long startTime = System.currentTimeMillis();

        for (String url : seedUrls) {
            submitCrawlTask(url, 0);
        }

        // Wait until no tasks are active AND no more work is being submitted
        try {
            while (activeTasks.get() > 0 && pagesCrawled.get() < MAX_PAGES_TO_CRAWL) {
                Thread.sleep(200);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

        long endTime = System.currentTimeMillis();
        printSummary(endTime - startTime);
    }

    private void submitCrawlTask(String url, int depth) {
        if (depth > MAX_DEPTH) return;
        if (pagesCrawled.get() >= MAX_PAGES_TO_CRAWL) return;

        // Atomic check-and-add: only one thread can successfully "claim" a URL
        if (!visitedUrls.add(url)) return;

        activeTasks.incrementAndGet();
        executor.submit(() -> {
            try {
                crawlPage(url, depth);
            } finally {
                activeTasks.decrementAndGet();
            }
        });
    }

    private void crawlPage(String url, int depth) {
        if (pagesCrawled.get() >= MAX_PAGES_TO_CRAWL) return;

        String threadName = Thread.currentThread().getName();
        try {
            System.out.printf("[%s] Crawling (depth %d): %s%n", threadName, depth, url);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(java.time.Duration.ofSeconds(5))
                    .header("User-Agent", "Mozilla/5.0 (SimpleJavaCrawler)")
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            String html = response.body();

            int count = pagesCrawled.incrementAndGet();
            System.out.printf("[%s] ✓ Fetched (%d/%d) [%d bytes] %s%n",
                    threadName, count, MAX_PAGES_TO_CRAWL, html.length(), url);

            indexPage(url, html);

            List<String> links = extractLinks(html);
            for (String link : links) {
                submitCrawlTask(link, depth + 1);
            }

        } catch (IOException | InterruptedException e) {
            System.out.printf("[%s] ✗ Failed to fetch %s (%s)%n", threadName, url, e.getMessage());
        }
    }

    private List<String> extractLinks(String html) {
        List<String> links = new ArrayList<>();
        Matcher matcher = LINK_PATTERN.matcher(html);
        while (matcher.find() && links.size() < 20) { // cap links per page
            links.add(matcher.group(1));
        }
        return links;
    }

    private void indexPage(String url, String html) {
        // Strip HTML tags crudely, then split into words
        String text = html.replaceAll("<[^>]+>", " ")
                           .replaceAll("[^a-zA-Z\\s]", " ")
                           .toLowerCase();
        String[] words = text.split("\\s+");

        for (String word : words) {
            if (word.length() < 4) continue; // skip tiny/noise words
            invertedIndex.computeIfAbsent(word, k -> ConcurrentHashMap.newKeySet()).add(url);
        }
    }

    private void printSummary(long elapsedMs) {
        System.out.println("\n=============================================");
        System.out.println("=== Crawl Complete ===");
        System.out.println("Pages crawled : " + pagesCrawled.get());
        System.out.println("Unique URLs seen : " + visitedUrls.size());
        System.out.println("Indexed words : " + invertedIndex.size());
        System.out.println("Time taken : " + elapsedMs + " ms");
        System.out.println("=============================================\n");

        // Simple search demo
        searchDemo("wikipedia");
        searchDemo("example");
    }

    private void searchDemo(String keyword) {
        Set<String> results = invertedIndex.getOrDefault(keyword.toLowerCase(), Set.of());
        System.out.println("Search results for \"" + keyword + "\": " + results.size() + " page(s) found");
        results.forEach(url -> System.out.println("   -> " + url));
        System.out.println();
    }
}