# SQLite durability for single-node engines

SQLite is a good fit for a single-node durable execution engine because a transaction can append events, update an index of timers and change a status row atomically. Write-ahead logging (WAL mode) lets readers continue while a writer appends, and keeps commits cheap because new pages are appended to the log file.

The synchronous setting decides when a commit is safe. With synchronous set to FULL, a committed transaction survives both a process crash and a power loss. With NORMAL in WAL mode, a commit survives a process crash but the most recent transactions may be lost on power failure. Benchmarks should report which setting they used.

A process killed in the middle of a transaction leaves no partial state: the uncommitted transaction is rolled back on the next open, which is the property crash-recovery logic relies on.
