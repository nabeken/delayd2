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

SELECT queue_id, payload
FROM
  queue
WHERE
      worker_id = 'worker-1'
  AND release_at < now()
;
```

Moving to active:

```sql
INSERT
INTO
  active
SELECT queue_id, worker_id
FROM
  queue
WHERE
  worker_id = 'worker-1'
  AND release_at < now()
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

Deleting queues from active and queue:

```sql
DELETE
FROM
  queue
WHERE
  queue_id IN ('queue-1', 'queue-2', 'queue-3')
;
```
