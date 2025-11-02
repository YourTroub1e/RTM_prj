from database import get_robots, update_robots, get_products, history, update_real_time_statistic
import random
import os
import time



class Robot:

    def __init__(self, robot_id, battery):

        self.depends = {
            "1": "A",
            "2": "D",
            "3": "G",
            "4": "J",
            "5": "M"
        }

        self.robot_id = robot_id
        self.battery = battery
        self.start_zone = self.depends[robot_id[-1]]
        self.current_zone = self.start_zone
        self.current_row = 1
        self.current_shelf = 1
        self.status = "active"

    def generate_scan_data(self):

        scanned_products = get_products()
        scan_results = []

        for product in scanned_products:
            quantity = random.randint(5, 100)
            status = "ok" if quantity > product[4] else ("low_stock" if quantity > product[3] else "critical")

            scan_results.append({
                "product_id": product[0],
                "product_name": product[1],
                "quantity": quantity,
                "status": status
            })

        return scan_results

    def move_to_next_location(self):

        self.current_shelf += 1

        if self.current_shelf > 10:
            self.current_shelf = 1
            self.current_row += 1

            if self.current_row > 8:
                self.current_row = 1

                finish_zone = chr(ord(self.start_zone) + 2)
                self.current_zone = chr(ord(self.current_zone) + 1)
                if ord(self.current_zone) > ord(finish_zone):
                    self.current_zone = 'Z'
                    self.status = "charging"

        self.battery -= random.uniform(0.1, 0.5)

    def charging(self):
        self.battery += random.uniform(0.1, 0.5)
        if self.battery >= 100:
            self.battery = 100
            self.status = "active"
            self.current_zone = self.start_zone
        update_robots(self.robot_id, "charging", 'Z', 0, 0, self.battery)

    def send_data(self):

        scan_result = self.generate_scan_data()
        for product in scan_result:
            history(self.robot_id, product["product_id"], product["quantity"], self.current_zone, self.current_row, self.current_shelf, product["status"])
        update_robots(self.robot_id, "active", self.current_zone, self.current_row, self.current_shelf, self.battery)



    def run(self):

        while True:
            if self.status == "active":
                self.send_data()
                self.move_to_next_location()
            elif self.status == "charging":
                self.charging()
            update_real_time_statistic()
            time.sleep(int(os.getenv('UPDATE_INTERVAL', random.uniform(5, 7))))

if __name__ == "__main__":

    import threading

    data = get_robots()

    for i in range(5):
        robot = Robot(robot_id=data[i][0], battery=data[i][1])
        thread = threading.Thread(target=robot.run)
        thread.daemon = True
        thread.start()

    while True:
        time.sleep(5)