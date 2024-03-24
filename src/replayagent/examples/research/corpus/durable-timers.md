# Durable timers

A durable timer is a sleep that survives restarts. Instead of blocking a thread, the workflow records that it wants to wake at a given time. A scheduler keeps an index of due times and appends a timer-fired event when the time arrives. If the worker crashes while a timer is pending, the index is still on disk and the timer fires on schedule after recovery.

The same mechanism implements retry backoff, activity timeouts and signal deadlines. Keeping all due times in one indexed table makes finding the next due timer a logarithmic lookup. Tests can replace the clock with a virtual one that jumps straight to the next due time, so a workflow that sleeps for a day can be tested in milliseconds.
