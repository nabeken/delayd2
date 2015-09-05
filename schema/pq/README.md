# schema for PostgreSQL

Tested with PostgreSQL 9.4.

## Examples

Queueing:

```sql
INSERT INTO queue VALUES ('queue-1', 'worker-1', now(), 'next', '{"say": "hi"}');
INSERT INTO queue VALUES ('queue-2', 'worker-1', now(), 'next', '{"say": "hi"}');
INSERT INTO queue VALUES ('queue-3', 'worker-1', now(), 'next', '{"say": "hi"}');

INSERT INTO queue VALUES ('queue-4', 'worker-2', now(), 'next', '{"say": "hi"}');
INSERT INTO queue VALUES ('queue-5', 'worker-2', now(), 'next', '{"say": "hi"}');
INSERT INTO queue VALUES ('queue-6', 'worker-3', now(), 'next', '{"say": "hi"}');
```

Moving to active:

```sql
INSERT
INTO
  active
SELECT queue.queue_id, queue.payload
FROM
  queue
WHERE
      queue.worker_id = 'worker-1'
  AND queue.release_at < now()
  AND NOT EXISTS (
    SELECT 1 FROM active WHERE queue_id = queue.queue_id
  )
;
```

Retrieving active messages:

```sql
SELECT queue.queue_id, queue.worker_id, queue.release_at, queue.relay_to, queue.payload
FROM
  queue INNER JOIN active USING (queue_id)
WHERE
  queue.worker_id = 'worker-1'
;
```

Resetting active messages:

```sql
DELETE
FROM
  active
WHERE
  worker_id = 'worker-1'
;
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
