CREATE TABLE queue (
   queue_id   TEXT PRIMARY KEY
 , worker_id  TEXT NOT NULL
 , release_at TIMESTAMP WITH TIME ZONE NOT NULL
 , payload    TEXT NOT NULL
);

CREATE TABLE active (
   queue_id   TEXT PRIMARY KEY
 , worker_id  TEXT NOT NULL
 , begin_at   TIMESTAMP WITH TIME ZONE DEFAULT now()

 , FOREIGN KEY (queue_id) REFERENCES queue (queue_id) ON DELETE CASCADE
);
