# AI Tool Usage

## Tools I Used

- **Claude (Anthropic), via the Claude Code CLI in the terminal** — used as a
  pair-programming assistant for scaffolding the FastAPI/SQLAlchemy/Redis
  boilerplate, drafting the docker-compose setup, generating the first pass of
  the test suite, and running an adversarial multi-agent code review of the
  concurrency logic.
- Editor: VS Code with its Python/Pylance language server for inline type
  checking while integrating and reviewing the generated code.

## What Helped Most

1. **Boilerplate velocity.** Wiring up the async SQLAlchemy 2.0 models, the
   FastAPI app factory with an injectable session/queue (so tests don't need a
   real database), and the Dockerfile/compose files is mechanical work that AI
   accelerates a lot. I described the architecture I wanted and reviewed the
   generated code rather than typing it out.

2. **Test scaffolding.** Getting `pytest-asyncio` + `httpx.ASGITransport` +
   `fakeredis` + in-memory SQLite to cooperate has a lot of fiddly fixture
   plumbing. AI produced a working `conftest.py` skeleton quickly, which I then
   shaped to match the exact scenarios the assignment requires.

## What I Had to Fix

These are the cases where AI gave plausible-but-wrong distributed-systems
advice, which is exactly what this assignment probes:

1. **"Just use `BRPOPLPUSH`/`BZPOPMAX` and you're done — the atomic pop
   guarantees a job runs once."** This is subtly wrong. The atomic pop prevents
   *two workers grabbing the same queued id*, but it says nothing about a job
   that was **cancelled** after enqueue, or one the **reaper re-enqueued** after
   a presumed crash. Without a second guard you can process a cancelled job, or
   process a job twice after recovery. I added the Postgres compare-and-set
   claim (`UPDATE … WHERE status IN ('pending','scheduled')`) as the
   reconciling guard, and made the completion step a `worker_id`-scoped CAS so a
   reaped-then-revived slow worker can't clobber the new owner's result.

2. **Incrementing `attempts` on failure instead of on claim.** The first draft
   bumped the attempt counter when a job *failed*. That leaves a poison job that
   hard-crashes the worker (never reaching the failure handler) with its counter
   stuck at 0 forever — an infinite crash loop that a heartbeat reaper would
   keep resurrecting. I moved the increment into the claim `UPDATE`, so a job
   that repeatedly kills workers still exhausts its budget and lands in the DLQ.

3. **Scheduler race between two workers.** The naive "read due jobs, then
   enqueue them" promotion loop double-enqueues under concurrency and has a
   torn-write window between enqueue and status flip. I made every worker's
   maintenance sweep election-gated by a Redis lock, ordered the promotion
   enqueue-before-flip, and made the claim accept `scheduled`-and-due rows so a
   crash mid-promotion still converges rather than losing the job.

## What AI Struggled With

- **Reasoning about the *interaction* of failure modes.** AI is good at any
  single mechanism in isolation (a heartbeat, a lock, a CAS) but tended to
  propose them as independent bolt-ons without noticing they have to compose:
  e.g. that "increment attempts on claim" is what makes crash recovery and
  poison-message defence work *together*, or that at-least-once delivery forces
  the completion write to be `worker_id`-scoped. Getting the guarantees to line
  up end-to-end (exactly which transition is the CAS, what invariant each index
  protects) was the part I had to drive and verify myself.
- **Float score packing for the priority ZSET.** AI's first suggestion packed
  priority and timestamp in a way that silently overflowed float64 integer
  precision for large timestamps. I reworked the band width and the
  timestamp-subtraction tiebreak so the packed score stays exact.
