import csv
import pika
import time

# RabbitMQ connection parameters
RABBITMQ_HOST = 'localhost'

def read_temperature_data(filename):
    data = []
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            data.append(row)
    return data

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    temperature_data = read_temperature_data('smoker-temps.csv')

    for row in temperature_data:
        message = f"Time: {row[0]}, Smoker: {row[1]}, FoodA: {row[2]}, FoodB: {row[3]}"
        channel.basic_publish(
            exchange='',
            routing_key='01-smoker',
            body=message
        )
        channel.basic_publish(
            exchange='',
            routing_key='02-food-A',
            body=message
        )
        channel.basic_publish(
            exchange='',
            routing_key='03-food-B',
            body=message
        )
        print(f"Sent: {message}")
        time.sleep(30)  # Sleep for 30 seconds between readings

    connection.close()

if __name__ == '__main__':
    main()
