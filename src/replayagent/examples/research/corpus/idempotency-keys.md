# Idempotency keys and duplicate side effects

A crash can happen after a side effect took place but before its result was recorded. On recovery the step is executed again, so durable execution engines give at-least-once execution for steps. To avoid duplicate side effects, a step passes an idempotency key to the system it changes. The key is derived from stable identifiers such as the workflow id and the step position, never from the attempt number or the clock.

The receiving system stores the key together with the effect, for example with a unique constraint and an insert that ignores conflicts, or by returning the stored response for a repeated key. Payment providers and many messaging APIs accept idempotency keys for exactly this reason.

When a system cannot deduplicate, the alternative is at-most-once execution: record that the step started before running it, and if a crash leaves the outcome unknown, report the uncertainty to the workflow instead of running the step again.
