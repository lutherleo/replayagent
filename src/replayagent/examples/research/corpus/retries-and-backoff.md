# Retries with exponential backoff

Transient failures such as timeouts, connection resets and rate limits are normal when an agent calls models and tools over a network. A retry policy describes how many attempts a step may make and how long to wait between them. Exponential backoff multiplies the wait after each failure, for example one second, then two, then four, up to a maximum interval.

Jitter adds a random factor to each wait so that many clients that failed together do not retry together and overload the service again. Some errors should never be retried: a request rejected as malformed will fail the same way every time. Policies therefore list non-retryable error types.

In a durable engine the retry schedule itself must survive crashes. The time of the next attempt is written to the history, so a restarted worker resumes the countdown instead of starting over or retrying immediately.
