import pika

# RabbitMQ connection parameters
RABBITMQ_HOST = 'localhost'

def callback(ch, method, properties, body):
    message = body.decode('utf-8')
    print(f"Food A Received: {message}")

    # Check for food stall alerts
    if "Food A Temp:" in message:
        food_a_temp = float(message.split(",")[2].split(":")[1].strip())
        if food_a_temp <= 1:
            print("Food A Stall Alert!")

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue='02-food-A',durable=True)

    channel.basic_consume(queue='02-food-A', on_message_callback=callback, auto_ack=True)

    print("Food A Monitor: Waiting for messages. To exit, press Ctrl+C")
    channel.start_consuming()

if __name__ == '__main__':
    main()
