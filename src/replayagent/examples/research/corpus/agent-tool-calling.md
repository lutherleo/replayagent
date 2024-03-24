# Tool calling in LLM agents

A tool-calling agent alternates between model turns and tool executions. The model receives the conversation plus a list of tools described by JSON Schema, and replies either with text or with one or more tool calls. The runtime executes each call, appends the results to the conversation as tool messages and asks the model again, until the model answers without calling a tool or a turn limit is reached.

Every model call costs money and time, and every tool call may change the outside world. Recording each turn and each tool result in a durable history means a crashed agent resumes from the last completed step without paying for earlier model calls again and without repeating tool side effects. Independent tool calls from one turn can run in parallel. Limiting the number of turns and forcing a final answer on the last turn keeps loops bounded.
