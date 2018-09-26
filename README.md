# kafka-transactions
Sandbox to play with kafka transactions

A `consumer.poll()` retrieves records from multiple event streams. If the processing of a particular event stream fails I don't want to rollback the transaction
for the entire set of Records. So probably the transaction would be around one record, not all.

Another problem is that if the processing of a record fails and the processing of other event in the same partition works the offset
will be committed and there is no way to replay the failed event.

