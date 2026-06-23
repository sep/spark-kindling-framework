package io.kindling.abfss;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link AzureCliTokenProvider}.
 *
 * Uses a testable subclass that overrides {@code fetchToken()} to avoid
 * invoking the real {@code az} CLI.
 */
public class AzureCliTokenProviderTest {

    // -----------------------------------------------------------------------
    // Testable subclass
    // -----------------------------------------------------------------------

    static class TestableProvider extends AzureCliTokenProvider {

        int fetchCount = 0;
        String tokenToReturn;
        Date   expiryToReturn;

        TestableProvider(String token, Date expiry) {
            this.tokenToReturn  = token;
            this.expiryToReturn = expiry;
        }

        @Override
        void fetchToken() throws IOException {
            fetchCount++;
            // Simulate what the real fetchToken sets via reflection-accessible fields.
            // Because the fields are package-private we set them directly from the same package.
            try {
                java.lang.reflect.Field tf = AzureCliTokenProvider.class.getDeclaredField("cachedToken");
                tf.setAccessible(true);
                tf.set(this, tokenToReturn);

                java.lang.reflect.Field ef = AzureCliTokenProvider.class.getDeclaredField("cachedExpiry");
                ef.setAccessible(true);
                ef.set(this, expiryToReturn);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Configuration conf;

    @Before
    public void setUp() throws IOException {
        conf = new Configuration(false);
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    private TestableProvider newProvider(String token, Date expiry) throws IOException {
        TestableProvider p = new TestableProvider(token, expiry);
        p.initialize(conf, "testaccount");
        return p;
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /**
     * A second call within the cache window must not trigger re-fetch.
     */
    @Test
    public void testCacheHit_secondCallDoesNotRefetch() throws IOException {
        // expiry is 10 minutes from now — well outside the 5-minute buffer
        Date expiry = new Date(System.currentTimeMillis() + 600_000L);
        TestableProvider p = newProvider("token-A", expiry);

        String first  = p.getAccessToken();
        String second = p.getAccessToken();

        assertEquals("token-A", first);
        assertEquals("token-A", second);
        assertEquals("fetchToken should be called exactly once", 1, p.fetchCount);
    }

    /**
     * When the cached token is within the refresh buffer, a call to
     * {@code getAccessToken()} must trigger re-fetch before returning.
     */
    @Test
    public void testCacheMiss_withinRefreshBufferTriggersFetch() throws IOException {
        // First expiry is only 1 minute away — inside the 5-minute default buffer
        Date nearExpiry = new Date(System.currentTimeMillis() + 60_000L);
        TestableProvider p = newProvider("token-stale", nearExpiry);

        // Prime the cache with a stale token
        p.fetchCount = 0;
        // Manually set the cache to simulate a stale state (bypasses getAccessToken logic)
        try {
            java.lang.reflect.Field tf = AzureCliTokenProvider.class.getDeclaredField("cachedToken");
            tf.setAccessible(true);
            tf.set(p, "token-stale");

            java.lang.reflect.Field ef = AzureCliTokenProvider.class.getDeclaredField("cachedExpiry");
            ef.setAccessible(true);
            ef.set(p, nearExpiry);
        } catch (Exception e) {
            fail("Reflection setup failed: " + e.getMessage());
        }

        // Update what the mock fetch will return
        p.tokenToReturn  = "token-fresh";
        p.expiryToReturn = new Date(System.currentTimeMillis() + 3600_000L);

        String result = p.getAccessToken();

        assertEquals("Should return refreshed token", "token-fresh", result);
        assertEquals("fetchToken should have been called once for refresh", 1, p.fetchCount);
    }

    /**
     * Expiry parsing with the {@code expiresOn} field (older az CLI format).
     */
    @Test
    public void testExpiryParsing_expiresOnField() throws IOException {
        // expiresOn uses "yyyy-MM-dd HH:mm:ss.SSSSSS" format
        String json = "{\n"
                + "  \"accessToken\": \"tok-abc\",\n"
                + "  \"expiresOn\": \"2026-06-23 15:30:00.123456\"\n"
                + "}";

        // Use a subclass that runs the real parseAndCache but doesn't shell out
        TestableRealParse p = new TestableRealParse(json);
        p.initialize(conf, "testaccount");
        p.getAccessToken();

        Date expiry = p.getExpiryTime();
        assertNotNull("Expiry should not be null", expiry);

        // The parsed date should be in 2026
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.setTime(expiry);
        assertEquals(2026, cal.get(java.util.Calendar.YEAR));
        assertEquals(java.util.Calendar.JUNE, cal.get(java.util.Calendar.MONTH));
        assertEquals(23, cal.get(java.util.Calendar.DAY_OF_MONTH));
    }

    /**
     * Expiry parsing with the {@code expires_on} field (newer az CLI format).
     */
    @Test
    public void testExpiryParsing_expiresOnSnakeField() throws IOException {
        // expires_on uses ISO-8601 format in newer CLI versions
        String json = "{\n"
                + "  \"accessToken\": \"tok-xyz\",\n"
                + "  \"expires_on\": \"2026-07-01T10:00:00Z\"\n"
                + "}";

        TestableRealParse p = new TestableRealParse(json);
        p.initialize(conf, "testaccount");
        p.getAccessToken();

        Date expiry = p.getExpiryTime();
        assertNotNull("Expiry should not be null for expires_on field", expiry);

        java.util.Calendar cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
        cal.setTime(expiry);
        assertEquals(2026, cal.get(java.util.Calendar.YEAR));
        assertEquals(java.util.Calendar.JULY, cal.get(java.util.Calendar.MONTH));
        assertEquals(1, cal.get(java.util.Calendar.DAY_OF_MONTH));
    }

    // -----------------------------------------------------------------------
    // Helper subclass that runs real parseAndCache with injected JSON
    // -----------------------------------------------------------------------

    static class TestableRealParse extends AzureCliTokenProvider {
        private final String fakeJson;
        int fetchCount = 0;

        TestableRealParse(String json) {
            this.fakeJson = json;
        }

        @Override
        void fetchToken() throws IOException {
            fetchCount++;
            // Invoke the real parseAndCache via a package-accessible approach:
            // We replicate the parse call by invoking the private method via reflection.
            try {
                java.lang.reflect.Method m = AzureCliTokenProvider.class
                        .getDeclaredMethod("parseAndCache", String.class);
                m.setAccessible(true);
                m.invoke(this, fakeJson);
            } catch (java.lang.reflect.InvocationTargetException ite) {
                Throwable cause = ite.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                throw new IOException("parseAndCache failed", cause);
            } catch (Exception e) {
                throw new IOException("Reflection error", e);
            }
        }
    }
}
