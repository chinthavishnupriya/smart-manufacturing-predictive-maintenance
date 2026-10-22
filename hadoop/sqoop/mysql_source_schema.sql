-- Demo source schema for the capstone MySQL -> Sqoop -> HDFS path.
-- Run in a non-production/demo MySQL database.
CREATE DATABASE IF NOT EXISTS smart_manufacturing;
USE smart_manufacturing;

CREATE TABLE IF NOT EXISTS machine_master (
  machine_id VARCHAR(64) NOT NULL PRIMARY KEY,
  plant VARCHAR(64) NOT NULL,
  machine_type VARCHAR(64) NOT NULL,
  installed_at DATETIME NULL,
  machine_status VARCHAR(24) NOT NULL DEFAULT 'ACTIVE'
);

CREATE TABLE IF NOT EXISTS maintenance_history (
  maintenance_id BIGINT NOT NULL PRIMARY KEY,
  machine_id VARCHAR(64) NOT NULL,
  maintenance_time DATETIME NOT NULL,
  maintenance_type VARCHAR(64) NOT NULL,
  notes VARCHAR(512) NULL,
  INDEX idx_maintenance_machine (machine_id),
  CONSTRAINT fk_maintenance_machine FOREIGN KEY (machine_id)
    REFERENCES machine_master(machine_id)
);

CREATE TABLE IF NOT EXISTS work_orders (
  work_order_id BIGINT NOT NULL PRIMARY KEY,
  machine_id VARCHAR(64) NOT NULL,
  created_at DATETIME NOT NULL,
  failure_probability DECIMAL(6,5) NOT NULL,
  status VARCHAR(24) NOT NULL,
  description VARCHAR(512) NOT NULL,
  INDEX idx_work_order_machine (machine_id),
  CONSTRAINT fk_work_order_machine FOREIGN KEY (machine_id)
    REFERENCES machine_master(machine_id)
);

INSERT INTO machine_master (machine_id, plant, machine_type, installed_at, machine_status)
VALUES
  ('M001', 'Plant-A', 'Motor', '2022-01-10 09:00:00', 'ACTIVE'),
  ('M002', 'Plant-A', 'Pump', '2022-03-15 09:00:00', 'ACTIVE'),
  ('M003', 'Plant-B', 'Conveyor', '2021-11-20 09:00:00', 'ACTIVE')
ON DUPLICATE KEY UPDATE plant=VALUES(plant), machine_type=VALUES(machine_type),
  installed_at=VALUES(installed_at), machine_status=VALUES(machine_status);

INSERT INTO maintenance_history (maintenance_id, machine_id, maintenance_time, maintenance_type, notes)
VALUES
  (1001, 'M001', '2026-09-15 10:30:00', 'INSPECTION', 'Routine vibration check'),
  (1002, 'M002', '2026-09-20 14:00:00', 'LUBRICATION', 'Scheduled pump maintenance'),
  (1003, 'M003', '2026-10-01 08:15:00', 'REPAIR', 'Replaced worn belt')
ON DUPLICATE KEY UPDATE machine_id=VALUES(machine_id), maintenance_time=VALUES(maintenance_time),
  maintenance_type=VALUES(maintenance_type), notes=VALUES(notes);

INSERT INTO work_orders (work_order_id, machine_id, created_at, failure_probability, status, description)
VALUES
  (5001, 'M001', '2026-10-20 12:00:00', 0.91200, 'OPEN', 'Inspect elevated vibration and temperature'),
  (5002, 'M003', '2026-10-21 08:30:00', 0.83100, 'OPEN', 'Inspect conveyor belt wear')
ON DUPLICATE KEY UPDATE machine_id=VALUES(machine_id), created_at=VALUES(created_at),
  failure_probability=VALUES(failure_probability), status=VALUES(status), description=VALUES(description);
