# Sowdamini Nandigama - 10/1/2023
# streaming-06-consumer-alert
Ideally here we are creating, monitoring and alerting system for temperature-sensitive environments, such as a smoker and food smoking processes, using various callback scripts from the cosumer part.
If smoker temp decreases by 15 F or more in 2.5 min (or 5 readings) --> smoker alert!
If food temp change in temp is 1 F or less in 10 min (or 20 readings) --> food stall alert!
Prerequisites
Git
Python 3.7+ (3.11+ preferred)
VS Code Editor
VS Code Extension: Python (by Microsoft)

The following modules are required:

Module	Version
csv	1.0
webbrowser	3.11.4
sys	3.11.4
time	3.11.4
pika	1.3.2
collections	3.11.4
## Screenshot

![RabbitMQ - Powershell Queues - Producer, Consumer](https://github.com/SowdaminiN/module-06-consumers/blob/main/Powershell.png)
![RabbitMQ - Admin Queues - Producer, Consumer](https://github.com/SowdaminiN/module-06-consumers/blob/main/Rabbit_admin_UI.png)