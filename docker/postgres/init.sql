CREATE TABLE IF NOT EXISTS client (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    country VARCHAR(100),
    signup_date DATE,
    gender VARCHAR(20),
    age_range VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS product (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255),
    brand VARCHAR(100),
    category VARCHAR(100),
    cost_price DECIMAL(10, 2),
    color VARCHAR(50),
    size VARCHAR(10),
    catalog_price DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS sale (
    sale_id INTEGER PRIMARY KEY,
    sale_date DATE,
    channel VARCHAR(100),
    channel_campaigns VARCHAR(100),
    customer_id INTEGER,
    CONSTRAINT fk_sale_client FOREIGN KEY (customer_id) REFERENCES client(customer_id)
);

CREATE TABLE IF NOT EXISTS sale_product (
    item_id INTEGER PRIMARY KEY,
    sale_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    discount_applied DECIMAL(2, 2),
    CONSTRAINT fk_sale_product_sale FOREIGN KEY (sale_id) REFERENCES sale(sale_id),
    CONSTRAINT fk_sale_product_product FOREIGN KEY (product_id) REFERENCES product(product_id)
);

CREATE INDEX IF NOT EXISTS idx_sale_customer_id ON sale(customer_id);
CREATE INDEX IF NOT EXISTS idx_sale_product_sale_id ON sale_product(sale_id);
CREATE INDEX IF NOT EXISTS idx_sale_product_product_id ON sale_product(product_id);


CREATE VIEW sales AS
SELECT p.product_id, s.sale_id, c.customer_id, sp.item_id, s.sale_date, quantity, (quantity * catalog_price * (1 - discount_applied)) as sales_amount,
channel, channel_campaigns, sp.discount_applied
FROM sale_product sp
JOIN sale s ON s.sale_id = sp.sale_id
JOIN product p ON p.product_id = sp.product_id
JOIN client c ON c.customer_id = s.customer_id;
