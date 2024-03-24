# Sagas and compensation

A saga is a long-running transaction split into steps, where each step that changes the outside world has a compensating step that undoes it. If a later step fails permanently, the workflow runs the compensations for the completed steps in reverse order. Sagas replace distributed locks with explicit, recorded business logic.

Durable execution makes sagas easier to write because the list of completed steps is part of the history, so the compensation logic is an ordinary try and except block in workflow code. Compensations must themselves be idempotent, since they can be retried after a crash like any other step.
