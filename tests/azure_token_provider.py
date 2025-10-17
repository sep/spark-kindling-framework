"""
Simple Azure OAuth token provider for Hadoop Azure filesystem.
This bypasses the complex OAuth provider classes and directly provides the token.
"""


class SimpleAzureTokenProvider:
    """
    A minimal token provider that returns a pre-fetched OAuth token.
    This is used to bypass Hadoop Azure's complex OAuth provider initialization.
    """

    def __init__(self, token):
        self.token = token

    def getAccessToken(self):
        """Return the OAuth access token."""
        return self.token

    def getExpiry(self):
        """Return token expiry time (far future for testing)."""
        import time

        return str(int(time.time()) + 3600)  # 1 hour from now
