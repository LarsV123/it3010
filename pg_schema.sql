-- Schema for the PostgreSQL version of the experiment
-- As date/time is not used, we don't need to convert it to a better format.
BEGIN TRANSACTION;
DROP TABLE IF EXISTS trackpoint;
CREATE TABLE trackpoint (
  tp_id SERIAL PRIMARY KEY,
  tp_user INTEGER NOT NULL,
  tp_point GEOMETRY NOT NULL,
  tp_altitude FLOAT,
  tp_date VARCHAR(32),
  tp_time VARCHAR(32)
);
CREATE INDEX trackpoint_geom_index ON trackpoint USING GIST (tp_point);
COMMIT TRANSACTION;