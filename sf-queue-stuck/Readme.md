This module contains a single pseudo test which illustrates an issue with message flow between clustered brokers.

Run `org.iviliev.sfQueueStuck.TestPubSubGettingStuck.test` in your IDE and observe the logs:
```log
Received/Sent: [SLOW_1: 16/845, FASTEST_1: 17898/17898, FASTER_1: 17002/17003]
```

The `SLOW_1` topic gets stuck at 16 messages received and never progresses.
The other two seems like they do not get stuck _most_ of the time, however I've had runs where they also got stuck
after 45 minutes.

Issues:
1. The addresses should not get stuck. The `SLOW_1` should be progressing. When all consumers are busy, there is no exception returned to the client(seen in logs).
2. `broker.getActiveMQServerControl().listQueues` does not list the three queues. In production this means we cannot troubleshoot the queues state.