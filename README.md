# Spring Batch remote partitioning
This repo contains 2 examples for Spring Batch remote partitioning; one with Kafka and the other with AWS SQS.

## Running the examples
Both of the example projects are quite self contained so the running procedure is the same.

Open 4 terminal windows for one of the projects. 3 terminals will run 3 worker instances and the 4th will run the manager to partition the data.

For the worker instances, run the following:

```java
$ ./gradlew bootRun --args='--spring.profiles.active=worker'
```

For the manager instance, run the following:

```java
$ ./gradlew bootRun --args='--spring.profiles.active=manager'
```