# Human-in-the-loop approvals

Many agent workflows must pause for a person: approving a draft, confirming a purchase or answering a clarifying question. In a durable workflow the pause is an ordinary blocking operation that waits for an external signal. The workflow can wait for hours or days without holding a thread or a process, because its progress lives in the history rather than in memory.

Signals that arrive before the workflow starts waiting are buffered, so a fast reviewer cannot be lost to a race. A timeout turns an unanswered request into an explicit outcome, such as escalating to someone else or abandoning the action. Recording the signal payload in the history means a replay after a crash sees the same decision, with the same reviewer feedback, without asking the person again.
