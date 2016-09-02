# streamline

A library to simplify some common Java API tasks.

This is not a framework, quite the opposite.


Basic guidelines

1. No dependencies, just uses the *java* and *javax* packages
2. Java 8, try to offer interfaces via to Streams if possible
3. Stick to the basics, do not get too clever, no unnecessary wheel re-inventions


## streamline.sql

Issue SQL commands via JDBC using varargs instead of setParameter, adding Stream support as an option.
