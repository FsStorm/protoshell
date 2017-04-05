# Protoshell [![Build status:](https://travis-ci.org/Prolucid/protoshell.svg?branch=master)](https://travis-ci.org/Prolucid/protoshell)
Loosely based on [Protoshell](https://github.com/jsgilmore/protoshell)

Protoshell implements Storm's multilang using the Protobuf serializer to improve throughput and decrease CPU when compared to the default JSON serializer.
It uses compact binary communications protocol and transfers tuples as union fields, mapping Protobuf supported types to Java types and vice versa.
When a type doesn't have Protobuf representation it is mapped to `byte[]`.

# Performance
Protoshell currently provides TBD throughput, when compared to the standard Storm JSON multilang protocol.

# The protocol
Protoshell captures multilang protocol as unions:
 - `StormMsg` for messages that could be sent from Storm
 - `ShellMsg` for messages that could be sent from a shell spout or a bolt

The exchange follows the documented [JSON script](http://storm.apache.org/documentation/Multilang-protocol.html).
