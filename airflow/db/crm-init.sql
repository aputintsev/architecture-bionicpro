CREATE TABLE customers (
    customer_id   BIGINT PRIMARY KEY,
    name          VARCHAR(255),
    email         VARCHAR(255),
    prosthetic_id VARCHAR(50),
    purchase_date DATE,
    order_total   NUMERIC(18, 2)
);

INSERT INTO customers VALUES
    (648821,  'Prothetic One',   'prothetic1@example.com', 'PROS-001', '2024-01-15', 85000.00),
    (6488214, 'Prothetic Two',   'prothetic2@example.com', 'PROS-002', '2024-02-20', 92000.00),
    (6488211, 'Prothetic Three', 'prothetic3@example.com', 'PROS-003', '2024-03-10', 78000.00);
