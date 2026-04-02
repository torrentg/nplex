# The Story Behind Nplex

This is not a technical document. It is the story of how Nplex came to be — the motivations,
the detours, the failures, and the lessons learned along the way.

## Where It Started

Two professional experiences planted the seed. Different companies, different teams, different
technologies — but a shared pattern: real-time data, shared state, SCADA-like scenarios where
dozens of components need to see the same picture at the same time. Both projects were
successful. Both shipped. Both left me with a quiet, persistent feeling that things could be
done better.

The first was a car chassis assembly line — robots controlled by PLCs, passing parts from
station to station. We built the entire supervision layer from scratch in roughly four months
using Splunk: real-time displays, KPIs, alarms, traceability, dashboards across multiple
screens. It was an all-in-one solution and it felt like magic. But the licensing costs were
steep, the server burned 20% CPU just to breathe, and the "real-time" was measured in seconds,
not milliseconds. For a dashboard that is fine. For closed-loop control, it is a different
story entirely.

The second was a control room for a metro line — a massive project spanning over four years,
around fifteen people full-time, across multiple sites. The stack was RabbitMQ with roughly
twelve Java micro-services handling a few hundred transactions per second at peak. Java was
the right choice for the organizational context: a large, rotating team where everyone could
read and contribute. But each "micro"-service consumed over a gigabyte of memory, took more
than thirty seconds to start, and the garbage collector introduced periodic freezes — visible
hiccups in a system that was supposed to feel alive. CPU consumption was, to put it
charitably, generous.

Both experiences were victories. The systems worked. The clients were satisfied. But I kept a
private list of things I would do differently if I ever had the chance to start from a blank
page — less overhead, true millisecond latency, a smaller footprint, and the kind of reactivity
that lets every component in the system respond to change the instant it happens.

## The Road to Nplex

That blank page, turned out to be a very long road, with a few dead ends.

The idea was simple, almost naïve: build a **reactive, high-performance key-value database** —
sub-millisecond commits, over 10,000 transactions per second — a kind of distributed spreadsheet
where every value could be a formula referencing other values, and modifying one would cascade
through its dependents in real time.

That idea took roughly **six years** to materialize into what Nplex is today. Not six years of
uninterrupted work — there were long stretches of inactivity, months where the project sat
untouched while life demanded attention elsewhere. But the thread was always there, waiting to
be picked up again: early mornings in the quiet hours before work, weekends carved out between 
errands, holidays spent refactoring instead of resting.

The first attempt — **memdb** — was not born from wisdom. It was born from impatience. Replication seemed like the most exciting part of the problem, so that is where I started — no server, no client, no database, just replication. And not by reading what others had already done. No. I would design my own consensus mechanism. Something better, obviously, than anything that had been peer-reviewed and battle-tested over decades. Looking back, the word that comes to mind is *reckless*. The codebase swelled past thirty thousand lines across more than a hundred substantial commits. And what did all that effort produce? A replication layer for a database that did not yet exist. memdb collapsed under its own ambition before a single key-value pair was ever stored.

The second attempt — **raft6** — was wiser, at least at first. This time I did the reading. I studied distributed systems properly, worked through the Raft paper, and understood the consensus problem from first principles. Raft — the algorithm I should have started with — was elegant, formally described, and exactly the kind of intellectual challenge I had been looking for. I embraced it with the energy of someone who had just discovered the right answer after a long detour. But if memdb had been defeated by arrogance, raft6 was defeated by architecture. Armed with new knowledge and a head full of design patterns, I tried to build the entire system behind clean hexagonal layers: isolate everything, abstract everything, make every boundary testable. It sounds beautiful in a conference talk. In practice, with an event loop at the core, it was an architectural nightmare. libuv wants to own its handles. The event loop *is* the application — trying to abstract it away is like trying to abstract away gravity. Fifteen thousand lines of code, two hundred and fifty commits, and the project drowned in its own layers of indirection.

The third attempt — **nplex** — was the one that stuck. After two failures, I adopted a deliberately pragmatic stance: no more chasing elegance for its own sake, no more building features before the foundation existed. Start with the basics — a server, a client, a store, a journal — and let complexity grow only when justified by real needs. Nplex reused parts of the previous attempts where they made sense and discarded the rest without ceremony. It was also the project where I decided to draw a line: ship what works, accept the limitations, and close the chapter. If it grows, it will be because someone else finds it useful — not because I cannot stop tinkering.

Along the way, several pieces of code proved self-contained enough to become independent libraries. They are children of Nplex — born from its needs, shaped by its constraints. They may lack the ambition of the parent project, but they are solid, tested, and public, and I am proud of each one of them.

| Project | Description | Why it exists |
|---------|-------------|---------------|
| [cstring](https://github.com/torrentg/cstring) | Immutable C-string with reference counting | `std::shared_ptr<std::string>` is heavy (control block, weak count, `size_t` overhead). Nplex uses cstring for values — automatic memory management, thread-safe, lightweight. |
| [journal](https://github.com/torrentg/journal) | Log-structured append-only storage | A full database (PostgreSQL, SQLite) would be overkill for sequential writes. journal is minimal, efficient, and purpose-built. |
| [cqueue](https://github.com/torrentg/cqueue) | Circular queue | Used in several places within Nplex. Surprisingly absent from the C++ standard library. |
| [expr](https://github.com/torrentg/expr) | Expression evaluator | Designed for the "computable values" part of the original vision — the spreadsheet dream. Not used in Nplex itself, but born from the same effort. |

## Lessons Learned

The lessons learned here are nothing extraordinary; they have been written and repeated in 
countless books and articles. But there is a world of difference between reading advice and living 
through the experience yourself. Only by making the mistakes firsthand did these lessons truly take 
root.

**Don't start from the roof.** It is tempting to begin with the most intellectually stimulating part of a project, but a replication layer without a database underneath is a roof without walls. I learned that you must build the boring, foundational parts first—those are what everything else stands on.

**Don't reinvent the wheel.** The temptation to build everything from scratch — your own algorithm, your own protocol, your own everything — is real, especially when you are excited about a problem. But someone else has almost certainly faced the same challenge before, thought it through more carefully, and written about it. Reading their work is not a shortcut or a sign of weakness. It is the only sensible starting point. I spent months designing a consensus algorithm that already existed in textbooks. The lesson cost me dearly.

**Beware of architecture astronautics.** My raft6 experience showed that trying to isolate every layer behind clean abstractions can backfire when the runtime model resists. Hexagonal architecture and an event loop that demands ownership of its handles are not natural allies. Sometimes, the pragmatic choice is to accept coupling where the framework imposes it.

**Don’t try to fit a square peg into a round hole.** libuv is written in C and imposes C-style ownership. I tried using uvw (a C++ wrapper) and fought it endlessly. The solution was to work with libuv's idioms, not against them. Likewise, disk access is best done directly with the system API—sometimes a simple write() is all you need.

**Trying to shape things too early often does more harm than good.** I tried to model keys with four hierarchical levels for optimized lookup. It was a solution to a problem that did not yet exist. Abstraction should come from evidence, not imagination.

**Objects are not always the answer.** Sometimes a struct is enough. Not everything needs a class with inheritance, encapsulation, and visitor patterns. Public visibility is not a sin if it is not exposed to the user. new/delete can be appropriate when ownership is clear.

**Use the tools the language provides.** I avoided dynamic polymorphism for fear of performance loss, and ended up with convoluted template machinery that saved nanoseconds in a system where the bottleneck was always the network or disk. A virtual method costs nothing meaningful when you are waiting for a TCP packet.

**Invest in the API.** The clearest measure of a library's quality is how simple it is to use, not how clever the implementation is. I spent significant time making the nplex-cpp client API as clear as possible, even if it meant more internal work.

**Test the right things.** Unit tests were straightforward. Functional tests for a client-server system were not. How do you test a server without a client? The Nplex answer: build the client in parallel, so it becomes both consumer and test harness.

**Avoid shiny distractions.** At one point I started writing a client in Node.js. It was interesting and educational, but a waste of time relative to the project's goals.

**Divide and conquer.** The best decision was to extract self-contained components into independent libraries, each with its own repository and tests. They look simple from the outside, but were not simple to build. Having them independent, tested, and public gave me great confidence in the project's foundation.

**Creating a product takes time.** Writing a small library that solves a problem can be relatively straightforward. But turning that library into a real product —with tests, documentation, examples, supporting tools, and all the polish that makes it usable and trustworthy— takes far more time and effort than the initial implementation. The gap between “it works for me” and “it’s ready for others” is wide, and closing it is real work.

## Final Thoughts

Nplex is the result of years of stolen hours. It works. It is fast. It is small. It does what
it set out to do. But it is also incomplete — there are features missing, rough edges unpolished,
and entire capabilities (TLS, admin console, replication) that exist only as lines in a TODO file.

Whether it grows beyond this point depends on whether anyone else finds it useful. If you are
reading this and see potential, reach out. If not, the project will remain as it is — a working
proof of concept and a personal record of what one person can build, stealing an hour of sleep 
each morning, day after day.
