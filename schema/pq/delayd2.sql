CREATE TABLE session (
   worker_id     TEXT PRIMARY KEY
 , keepalived_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE queue (
   queue_id   TEXT PRIMARY KEY
 , worker_id  TEXT NOT NULL
 , release_at TIMESTAMP WITH TIME ZONE NOT NULL
 , relay_to   TEXT NOT NULL
 , payload    TEXT NOT NULL
);

CREATE INDEX queue_index ON queue (
  worker_id, release_at
);

CREATE TABLE active (
   queue_id   TEXT PRIMARY KEY
 , worker_id  TEXT NOT NULL
 , begin_at   TIMESTAMP WITH TIME ZONE DEFAULT now()

 , FOREIGN KEY (queue_id) REFERENCES queue (queue_id) ON DELETE CASCADE
);
