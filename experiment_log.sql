-- Schema for the SQLite experiment log
DROP TABLE IF EXISTS result;
CREATE TABLE result (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dbms TEXT NOT NULL,
  indexed BOOLEAN NOT NULL,
  batch_size INTEGER NOT NULL,
  rows_inserted INTEGER NOT NULL,
  run_time FLOAT NOT NULL,
  git_hash TEXT NOT NULL,
  rows_per_second FLOAT GENERATED ALWAYS AS (ROUND(rows_inserted / run_time, 2)) STORED,
  ms_per_transaction FLOAT GENERATED ALWAYS AS (
    ROUND(
      (run_time * 1000) / (rows_inserted / batch_size),
      2
    )
  ) STORED,
  timestamp TEXT DEFAULT CURRENT_TIMESTAMP NOT NULL
);
DROP VIEW IF EXISTS result_summary;
CREATE VIEW result_summary AS
SELECT dbms,
  indexed,
  batch_size,
  ROUND(AVG(run_time), 2) AS avg_run_time,
  ROUND(AVG(ms_per_transaction), 2) AS avg_ms_per_transaction,
  ROUND(AVG(rows_per_second), 2) AS avg_rows_per_second,
  COUNT(*) AS runs
FROM result
GROUP BY dbms,
  indexed,
  batch_size
ORDER BY batch_size DESC;