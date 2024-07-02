-- queued background jobs
CREATE TABLE job_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- job type, string
    job_type TEXT NOT NULL,
    -- args of job, json object
    args TEXT NOT NULL,
    scheduled INTEGER NOT NULL,
    completed INTEGER,
    -- state of job - should be 'queued', 'running', 'failed', or 'completed'
    state TEXT NOT NULL,
    active INTEGER,
    -- output of job
    -- json, can be updated as job progresses.
    output TEXT
) STRICT;