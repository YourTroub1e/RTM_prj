import psycopg2
from random import randint as rand


connection = psycopg2.connect(database="rtk_it",
                        user="postgres",
                        password="0000",
                        host="localhost",
                        port="5432")


def get_robots():
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, battery_level FROM robots
            """
        )
        result = cursor.fetchall()

        return result

def update_robots(id_robot, status, zone, row, shelf, battery):
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE robots SET
            status = '{status}', 
            battery_level = {battery},
            last_update = CURRENT_TIMESTAMP,
            current_zone = '{zone}',
            current_row = {row},
            current_shelf = {shelf} 
            WHERE id = '{id_robot}'
            """
        )
        connection.commit()

def get_products():
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT id FROM products
            """
        )
        id_list = cursor.fetchall()
        picked = id_list[rand(0,4)][0]
        cursor.execute(
            f"""
            SELECT * FROM products WHERE id = '{picked}'
            """
        )
        result = cursor.fetchall()

        return result

def history(robot_id, product_id, quantity, zone, row_number, shelf_number, status):
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            INSERT INTO inventory_history (robot_id, product_id, quantity, zone, row_number, shelf_number, status, scanned_at) VALUES
            ('{robot_id}', '{product_id}', {quantity}, '{zone}', {row_number}, {shelf_number}, '{status}', CURRENT_TIMESTAMP);
            """
        )
        connection.commit()

def update_real_time_statistic():
    with connection.cursor() as cursor:
        cursor.execute(
            """
            WITH LatestScans AS (
                SELECT 
                    product_id,
                    quantity,
                    zone,
                    row_number,
                    shelf_number,
                    scanned_at,
                    status,
                    ROW_NUMBER() OVER (
                        PARTITION BY zone, row_number, shelf_number 
                        ORDER BY scanned_at DESC
                    ) as rn
                FROM inventory_history 
            ),
            RobotStats AS (
                SELECT 
                    COUNT(*) as total_robots,
                    COUNT(CASE WHEN status = 'active' THEN 1 END) as active_robots,
                    ROUND(AVG(battery_level)) as avg_battery_level
                FROM robots
            ),
            CriticalItems AS (
                SELECT 
                    COUNT(DISTINCT zone || '-' || row_number || '-' || shelf_number) as critical_items_count
                FROM LatestScans 
                WHERE rn = 1 AND status = 'critical'
            ),
            CheckedLocations AS (
                SELECT 
                    COUNT(DISTINCT zone || '-' || row_number || '-' || shelf_number) as total_checked_locations
                FROM LatestScans 
                WHERE rn = 1
            )
            UPDATE real_time_statistic 
            SET 
                active_robots = rs.active_robots,
                total_robots = rs.total_robots,
                total_checked_locations = cl.total_checked_locations,
                critical_items_count = ci.critical_items_count,
                avg_battery_level = rs.avg_battery_level
            FROM 
                RobotStats rs,
                CriticalItems ci,
                CheckedLocations cl;
            """
        )
        connection.commit()