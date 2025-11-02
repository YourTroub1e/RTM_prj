from sklearn.linear_model import LinearRegression
import numpy as np
import psycopg2
from datetime import date

connection = psycopg2.connect(database="rtk_it",
                        user="postgres",
                        password="0000",
                        host="localhost",
                        port="5432")

def get_total_quantity():
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
                    ROW_NUMBER() OVER (
                        PARTITION BY zone, row_number, shelf_number 
                        ORDER BY scanned_at DESC
                    ) as rn
                FROM inventory_history 
            )
            SELECT 
                l.product_id,
                SUM(l.quantity) as total_quantity,
                COUNT(*) as locations_count,
                p.optimal_stock
            FROM LatestScans l
            JOIN products p ON l.product_id = p.id
            WHERE l.rn = 1
            GROUP BY l.product_id, p.optimal_stock
            ORDER BY l.product_id;
            """
        )
        total = cursor.fetchall()

        return total

def table_ai_predictions(product_id, current_date, days_until_stockout, recommended_order):
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            INSERT INTO ai_predictions (product_id, prediction_date, days_until_stockout, recommended_order) VALUES
            ('{product_id}', '{current_date}', {days_until_stockout}, {recommended_order});
            """
        )
        connection.commit()

def ai_predict(current, optimal, days_passed=7):


    X = np.array([[0], [days_passed]])
    y = np.array([optimal, current])

    model = LinearRegression()
    model.fit(X, y)

    daily_consumption = -model.coef_[0]

    days_until_stockout = current / daily_consumption if daily_consumption > 0 else float('inf')

    recommended_order = daily_consumption * 7

    return days_until_stockout, recommended_order


def main():

    try:
        products = get_total_quantity()
        for item in products:
            days, order = ai_predict(item[1], item[2]*item[3])
            prediction_date = date.today()
            table_ai_predictions(item[0], prediction_date, round(float(days)), round(float(order)))

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()

