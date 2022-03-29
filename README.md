# Spring Batch remote partitioning
This repo contains 2 examples for Spring Batch remote partitioning; one with Kafka and the other with AWS SQS.

The Kafka example is explained in this article: [Scaling Spring Batch processing with partitioning using Kafka](https://arnoldgalovics.com/spring-batch-remote-partitioning-kafka/)

## Running the examples
Both of the example projects are quite self contained so the running procedure is the same.

First of all, start the necessary infrastructure using docker-compose by running `docker-compose up` in the project's folder. This will start up either a Kafka cluster with Kafka UI along with a MySQL database; or for the AWS SQS case, it'll start up localstack to be able to emulate SQS locally along with a MySQL database.

Then, open 4 terminal windows for one of the projects. 3 terminals will run 3 worker instances and the 4th will run the manager to partition the data.

For the worker instances, run the following:

```java
$ ./gradlew bootRun --args='--spring.profiles.active=worker'
```

For the manager instance, run the following:

```java
$ ./gradlew bootRun --args='--spring.profiles.active=manager'
```