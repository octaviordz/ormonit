# ormonit

Project created as a learning exercise for F# language and nanomsg library. Please don't consider this idiomatic F# code.


Ormonit allows you to create a set of services that run on different OS processes.

# Feature list

* Each service runs on independent OS process. [Implemented]
* Dynamic loading. [Implemented]
* Interprocess communication [WIP]
* Autodetect and load new services. [NOT Implemented]
* Supervisor. [WIP]
    * Collect service status. [NOT Implemented]
    * Automatic restart service when needed. [Implemented]
