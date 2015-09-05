# schema for PostgreSQL

Tested with PostgreSQL 9.4.

## Examples

Queueing:

```sql
INSERT INTO queue VALUES ('queue-1', 'worker-1', now(), '{"say": "hi"}');
INSERT INTO queue VALUES ('queue-2', 'worker-1', now(), '{"say": "hi"}');
INSERT INTO queue VALUES ('queue-3', 'worker-1', now(), '{"say": "hi"}');
```

Moving to active:

```sql
INSERT INTO active VALUES ('queue-1', 'worker-1');
INSERT INTO active VALUES ('queue-2', 'worker-1');
INSERT INTO active VALUES ('queue-3', 'worker-1');
```

Deleting queues from active and queue:

```sql
DELETE
FROM
  queue
WHERE
  queue_id IN ('queue-1', 'queue-2', 'queue-3')
;
```
