# Versioning workflow code

Workflows can run for days, so their code changes while instances are still in flight. A change that adds, removes or reorders durable operations breaks replay for instances started with the old code, because the recorded history no longer matches what the new code requests.

The standard remedy is a version marker. New code asks the runtime which version of a change applies to the current instance. For a new instance the runtime records the newest version and the code takes the new path. For an old instance the history has no marker at that position, so the runtime reports a default version and the code keeps the old path. Once every old instance has finished, the old branch can be deleted.

Teams protect themselves further by replaying recorded production histories against new code in their test suite before deploying it.
