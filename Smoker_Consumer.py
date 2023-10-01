import pika

# RabbitMQ connection parameters
RABBITMQ_HOST = 'localhost'

def callback(ch, method, properties, body):
    message = body.decode('utf-8')
    print(f"Smoker Received: {message}")

    # Check for smoker alert condition
    if "Smoker Temp:" in message:
        smoker_temp = float(message.split(",")[1].split(":")[1].strip())
        if smoker_temp < 15:
            print("Smoker Alert!")

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue='01-smoker',durable=True)

    channel.basic_consume(queue='01-smoker', on_message_callback=callback, auto_ack=True)

    print("Smoker Monitor: Waiting for messages. To exit, press Ctrl+C")
    channel.start_consuming()

if __name__ == '__main__':
    main()
