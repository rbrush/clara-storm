# clara-storm

A proof-of-concept [Clara Rules](https://github.com/rbrush/clara-rules) host that allows running of rules over a the [Storm processing system](http://storm-project.net).

## How it works
_NOTE: while building a more sophisticated test, I found the current model I used for mapping distributed memory onto Storm is flawed, in that by creating cycles in its processing topology it introduces the possibility of deadlock, where all bolts are blocked on downstream processing of the Rete network, which is blocked on those same bolts. There are ways to work around this like increasing the number of bolts available, but this is a workaround at best. A more permanent solution is needed._

See the [Clara Rules](https://github.com/rbrush/clara-rules) documentation for an understanding of the rules engine itself. This project distributes the engine's working memory across a Storm topology, making it possible to scale and process very large data streams with simple, declarative rules.

Clara makes distributed rules possible with some simple constraints in the engine itself. First, all facts used by Clara are immutable, which greatly simplifies sharing in a distributed environment

Second, Clara requires that all joins between facts are hash-based. For instance, if I want to join a TemperatureReading fact and a Location fact, they must share a field that can be joined. The clara-storm host will route all join operations with matching fact bindings to the same bolt, so the join can occur there. The [sensors example](https://github.com/rbrush/clara-examples/blob/master/src/clara/examples/sensors.clj) shows such joins in action.

This project models a distributed-memory rules engine in Storm by simply splitting it across an arbitrarily large number of _clara-bolts_. These bolts each hold a subset of the working memory, split up by hashing the fields that are used in rule joins. If a rule or constraint fires that creates new join values, they are sent to the appropriate _clara-bolt_ instance that contains that subset of the working memory.

The result is a processing topology that is somewhat atypical for Storm: rather than a deep graph of processing steps, we have a large number of peer clara-bolts responsible for a subset of data. These peers then share the output of their processing with eachother based on the hash-based joins.

## Usage

This is a proof-of-concept to exercise Clara over distributed systems. Usage information will come as this project progresses.

## License

Copyright © 2013 Ryan Brush

Distributed under the Eclipse Public License, the same as Clojure.