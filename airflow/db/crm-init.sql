CREATE TABLE customers (
    customer_id   BIGINT PRIMARY KEY,
    name          VARCHAR(255),
    email         VARCHAR(255),
    prosthetic_id VARCHAR(50),
    purchase_date DATE,
    order_total   NUMERIC(18, 2)
);

INSERT INTO customers VALUES
    (648821,  'Иван Петров',     'ivan.petrov@mail.ru',  'PROS-001', '2024-01-15', 85000.00),
    (6488214, 'Мария Сидорова',  'm.sidorova@mail.ru',   'PROS-002', '2024-02-20', 92000.00),
    (6488211, 'Алексей Козлов',  'a.kozlov@mail.ru',     'PROS-003', '2024-03-10', 78000.00),
    (6488219, 'Елена Новикова',  'e.novikova@mail.ru',   'PROS-004', '2024-04-05', 105000.00),
    (648801,  'Дмитрий Волков',  'd.volkov@mail.ru',     'PROS-005', '2024-05-18', 67000.00);
