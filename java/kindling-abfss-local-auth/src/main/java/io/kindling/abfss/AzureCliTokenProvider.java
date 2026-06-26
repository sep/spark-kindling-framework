package io.kindling.abfss;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hadoop CustomTokenProviderAdaptee that acquires Azure storage access tokens
 * by shelling out to the Azure CLI ({@code az account get-access-token}).
 *
 * <p>Intended for local development only. Tokens are cached in memory and
 * refreshed proactively when the remaining lifetime falls below the configured
 * buffer (default: 5 minutes).
 *
 * <p>Hadoop config keys:
 * <ul>
 *   <li>{@code fs.azure.account.oauth.provider.az.path} — path to the az binary (default: "az")</li>
 *   <li>{@code fs.azure.account.oauth.provider.refresh.buffer.seconds} — seconds before expiry
 *       to trigger a proactive refresh (default: 300)</li>
 * </ul>
 */
public class AzureCliTokenProvider implements CustomTokenProviderAdaptee {

    private static final Log LOG = LogFactory.getLog(AzureCliTokenProvider.class);

    /** Hadoop config key for the az CLI binary path. */
    static final String CONF_AZ_PATH = "fs.azure.account.oauth.provider.az.path";

    /** Hadoop config key for the proactive-refresh buffer in seconds. */
    static final String CONF_REFRESH_SECS = "fs.azure.account.oauth.provider.refresh.buffer.seconds";

    static final int DEFAULT_REFRESH_SECS = 300;

    private String azPath;
    private long refreshBufferMs;
    private String accountName;

    private String cachedToken;
    private Date   cachedExpiry;

    // Patterns used for manual JSON field extraction
    private static final Pattern TOKEN_PATTERN =
            Pattern.compile("\"accessToken\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern EXPIRES_ON_PATTERN =
            Pattern.compile("\"expiresOn\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern EXPIRES_ON_SNAKE_PATTERN =
            Pattern.compile("\"expires_on\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern TOKEN_EXPIRY_PATTERN =
            Pattern.compile("\"tokenExpiry\"\\s*:\\s*\"([^\"]+)\"");

    @Override
    public synchronized void initialize(Configuration conf, String accountName) throws IOException {
        this.accountName = accountName;
        this.azPath       = conf.get(CONF_AZ_PATH, "az");
        this.refreshBufferMs =
                conf.getLong(CONF_REFRESH_SECS, DEFAULT_REFRESH_SECS) * 1000L;
        LOG.debug("AzureCliTokenProvider initialized for account=" + accountName
                + " azPath=" + azPath
                + " refreshBufferMs=" + refreshBufferMs);
    }

    /**
     * Returns a valid access token for Azure Storage, refreshing if necessary.
     * Synchronized to prevent concurrent CLI invocations.
     */
    @Override
    public synchronized String getAccessToken() throws IOException {
        if (cachedToken != null && cachedExpiry != null
                && cachedExpiry.getTime() - System.currentTimeMillis() > refreshBufferMs) {
            return cachedToken;
        }
        fetchToken();
        return cachedToken;
    }

    /** Returns the expiry time of the currently cached token. */
    @Override
    public synchronized Date getExpiryTime() {
        return cachedExpiry;
    }

    /**
     * Invokes the Azure CLI and updates {@link #cachedToken} and {@link #cachedExpiry}.
     *
     * @throws IOException if the CLI is not found, returns a non-zero exit code,
     *                     or if the JSON output cannot be parsed.
     */
    void fetchToken() throws IOException {
        ProcessBuilder pb = new ProcessBuilder(
                azPath,
                "account", "get-access-token",
                "--resource", "https://storage.azure.com/",
                "-o", "json");
        pb.redirectErrorStream(false);

        Process process;
        try {
            process = pb.start();
        } catch (IOException e) {
            throw new IOException(
                    "Azure CLI not found at '" + azPath
                    + "'. Install az and run 'az login'.", e);
        }

        // Read stdout and stderr concurrently to avoid blocking on full pipe buffers.
        StringBuilder stdout = new StringBuilder();
        StringBuilder stderr = new StringBuilder();

        Thread stderrThread = new Thread(new Runnable() {
            public void run() {
                try {
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(process.getErrorStream()));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stderr.append(line).append('\n');
                    }
                } catch (IOException ignored) {
                    // ignore — stderr drain only
                }
            }
        });
        stderrThread.setDaemon(true);
        stderrThread.start();

        BufferedReader stdoutReader = new BufferedReader(
                new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = stdoutReader.readLine()) != null) {
            stdout.append(line).append('\n');
        }

        int exitCode;
        try {
            stderrThread.join(5000);
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for az CLI", e);
        }

        if (exitCode != 0) {
            String stderrMsg = stderr.toString().trim();
            LOG.warn("az account get-access-token failed (exit " + exitCode
                    + ") stderr: " + stderrMsg);
            throw new IOException(
                    "az account get-access-token failed (exit " + exitCode
                    + "). Run 'az login'. stderr: " + stderrMsg);
        }

        String json = stdout.toString();
        parseAndCache(json);
    }

    private void parseAndCache(String json) throws IOException {
        // Extract access token
        Matcher tokenMatcher = TOKEN_PATTERN.matcher(json);
        if (!tokenMatcher.find()) {
            throw new IOException(
                    "Could not parse accessToken from az output. Unexpected CLI format.");
        }
        String token = tokenMatcher.group(1);

        // Extract expiry — try expiresOn, expires_on, tokenExpiry in order
        Date expiry = null;
        String expiryStr = extractFirst(json,
                EXPIRES_ON_PATTERN,
                EXPIRES_ON_SNAKE_PATTERN,
                TOKEN_EXPIRY_PATTERN);

        if (expiryStr != null) {
            expiry = parseExpiry(expiryStr);
        }

        if (expiry == null) {
            // Fall back to 1-hour offset and log a warning
            expiry = new Date(System.currentTimeMillis() + 3600_000L);
            LOG.warn("Could not parse expiry from az output; defaulting to 1-hour from now.");
        }

        cachedToken  = token;
        cachedExpiry = expiry;
        LOG.info("AzureCliTokenProvider: token acquired for account=" + accountName
                + " expiry=" + cachedExpiry);
    }

    /**
     * Returns the first non-null group(1) match from the given patterns, or null.
     */
    private static String extractFirst(String json, Pattern... patterns) {
        for (Pattern p : patterns) {
            Matcher m = p.matcher(json);
            if (m.find()) {
                String val = m.group(1).trim();
                if (!val.isEmpty()) {
                    return val;
                }
            }
        }
        return null;
    }

    /**
     * Parses an expiry string as either {@code "yyyy-MM-dd HH:mm:ss.SSSSSS"} or ISO-8601.
     * Returns null if parsing fails.
     */
    private static Date parseExpiry(String raw) {
        if (raw == null || raw.isEmpty()) {
            return null;
        }

        // Try yyyy-MM-dd HH:mm:ss.SSSSSS format (older az CLI)
        if (raw.contains(" ") && raw.contains(".")) {
            try {
                // SimpleDateFormat with SSSSSS treats it as milliseconds * 1000;
                // truncate microseconds to milliseconds for compatibility.
                String normalized = raw;
                int dotIdx = raw.lastIndexOf('.');
                if (dotIdx >= 0) {
                    String fracPart = raw.substring(dotIdx + 1);
                    if (fracPart.length() > 3) {
                        // Truncate to 3 digits (milliseconds)
                        normalized = raw.substring(0, dotIdx + 1) + fracPart.substring(0, 3);
                    }
                }
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                return sdf.parse(normalized);
            } catch (ParseException e) {
                // fall through to ISO-8601 attempt
            }
        }

        // Try ISO-8601 (newer az CLI: "2025-06-23T14:30:00Z" or with offset)
        // java.util.Date doesn't support ISO-8601 directly in Java 8; handle common variants.
        try {
            // Normalize trailing Z and colon-form offsets (+HH:MM → +HHMM) for SimpleDateFormat
            String iso = raw.replace("Z", "+0000").replace("z", "+0000");
            iso = iso.replaceAll("([+-])(\\d{2}):(\\d{2})$", "$1$2$3");
            SimpleDateFormat isoFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
            return isoFmt.parse(iso);
        } catch (ParseException e) {
            // ignore
        }

        // Try without timezone
        try {
            SimpleDateFormat isoNoTz = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            return isoNoTz.parse(raw);
        } catch (ParseException e) {
            // ignore
        }

        return null;
    }
}
