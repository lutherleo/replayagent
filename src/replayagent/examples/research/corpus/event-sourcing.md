# Event sourcing for workflow state

Event sourcing stores every change to a piece of state as an immutable event in an append-only log, instead of overwriting a current-state row. The current state is derived by folding the events in order. For workflow engines the log is the workflow history: each scheduled step, each result and each timer is an event. Because the log is never rewritten, it doubles as an audit trail that shows exactly what an agent did and in which order.

Appending must be atomic and ordered. A common technique is optimistic concurrency: a writer states the sequence number it expects to write next, and the append fails if another writer got there first. This turns concurrent writers into a detectable conflict rather than silent corruption.

Folding a long log on every read is linear in its length, so engines cache the folded state in memory and only read the log in full after a restart.
