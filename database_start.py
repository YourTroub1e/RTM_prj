import psycopg2
from bcrypt import hashpw, gensalt

connection = psycopg2.connect(database="rtk_it",
                        user="postgres",
                        password="0000",
                        host="localhost",
                        port="5432")

def hashing_password(password):

    return str(hashpw(password.encode("utf-8"), gensalt()))[2:-1]


def start_db():
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            password VARCHAR(255) NOT NULL,
            name VARCHAR(255) NOT NULL,
            role VARCHAR(50) NOT NULL, -- 'operator', 'admin', 'viewer'
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS robots (
            id VARCHAR(50) PRIMARY KEY, -- 'RB-001'
            status VARCHAR(50) DEFAULT 'active',
            battery_level INTEGER,
            last_update TIMESTAMP,
            current_zone VARCHAR(10),
            current_row INTEGER,
            current_shelf INTEGER
            );

            CREATE TABLE IF NOT EXISTS products (
            id VARCHAR(50) PRIMARY KEY, -- 'TEL-4567'
            name VARCHAR(255) NOT NULL,
            category VARCHAR(100),
            min_stock INTEGER DEFAULT 10,
            optimal_stock INTEGER DEFAULT 100
            );

            CREATE TABLE IF NOT EXISTS inventory_history (
            id SERIAL PRIMARY KEY,
            robot_id VARCHAR(50) REFERENCES robots(id),
            product_id VARCHAR(50) REFERENCES products(id),
            quantity INTEGER NOT NULL,
            zone VARCHAR(10) NOT NULL,
            row_number INTEGER,
            shelf_number INTEGER,
            status VARCHAR(50), -- 'OK', 'LOW_STOCK', 'CRITICAL'
            scanned_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS ai_predictions (
            id SERIAL PRIMARY KEY,
            product_id VARCHAR(50) REFERENCES products(id),
            prediction_date DATE NOT NULL,
            days_until_stockout INTEGER,
            recommended_order INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS real_time_statistic (
            active_robots INTEGER NOT NULL,
            total_robots INTEGER NOT NULL,
            total_checked_locations INTEGER NOT NULL,
            critical_items_count INTEGER NOT NULL,
            avg_battery_level INTEGER NOT NULL
            );

            INSERT INTO users (email, password, name, role, created_at)
            VALUES ('abcd@yandex.ru', '{hashing_password("qwe")}', 'Peter', 'admin', CURRENT_TIMESTAMP);

            INSERT INTO robots (id, status, battery_level, current_zone) VALUES
            ('RB-001', 'charging', 100, 'Z'),
            ('RB-002', 'charging', 100, 'Z'),
            ('RB-003', 'charging', 100, 'Z'),
            ('RB-004', 'charging', 100, 'Z'),
            ('RB-005', 'charging', 100, 'Z');

            INSERT INTO products (id, name, category, min_stock, optimal_stock) VALUES
            ('TEL-4567', 'Роутер RT-AC68U', 'router', 35, 80),
            ('TEL-8901', 'Модем DSL-2640U', 'modem', 30, 75),
            ('TEL-2345', 'Коммутатор SG-108', 'switchboard', 30, 80),
            ('TEL-6789', 'IP-телефон T46S', 'ip_phone', 30, 70),
            ('TEL-3456', 'Кабель UTP Cat6', 'cable', 40, 90);

            INSERT INTO real_time_statistic (active_robots, total_robots, total_checked_locations, critical_items_count, avg_battery_level)
            VALUES (0, 0, 0, 0, 0);

            CREATE INDEX IF NOT EXISTS idx_inventory_scanned ON inventory_history(scanned_at DESC);
            CREATE INDEX IF NOT EXISTS idx_inventory_product ON inventory_history(product_id);
            CREATE INDEX IF NOT EXISTS idx_inventory_zone ON inventory_history(zone);
            """
        )
        connection.commit()

def main():
    start_db()

if __name__ == "__main__":
    main()
