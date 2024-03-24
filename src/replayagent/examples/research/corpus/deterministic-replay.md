# Deterministic replay

Deterministic replay rebuilds the in-memory state of a running program by re-executing its code against a recorded history. Each time the code asks for a durable operation, such as calling a tool or sleeping, the runtime first looks in the history. If the operation already happened, the recorded result is returned immediately and nothing is executed again.

Replay only works if the code is deterministic: given the same history it must request the same operations in the same order. Reading the wall clock, drawing random numbers, iterating over unordered sets or calling the network directly from workflow code all break that rule. Runtimes therefore offer replay-safe substitutes, such as a recorded clock and a seeded random generator.

A runtime detects non-determinism by comparing each requested operation with the recorded one. A mismatch in the operation type, name or arguments means the code changed in an incompatible way, and the safe reaction is to stop the workflow rather than guess.
