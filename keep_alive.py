import time
from datetime import datetime

while True:
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{current_time} I am alive! Going to sleep for 2 min!")
    time.sleep(120)  # Sleep for 2 minutes (2 * 60 seconds)