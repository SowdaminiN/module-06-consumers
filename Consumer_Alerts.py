"""
    This program will continuously look for the messages from Producer of Smoker, FoodA, FoodB
    and listens messages from mentioned queues above. 
"""

# Import project libraries
import pika
import time
import csv
import sys
from collections import deque
import json

# set deque max length
smoker_temperature = deque(maxlen=5)
food_a_temperature = deque(maxlen=20)
food_b_temperature = deque(maxlen=20)

# declare constants for temperature change
SMOKER_ALERT = 15.0 # smoker changes less than 15 degrees F in 2.5 minutes
FOOD_ALERT = 1.0 # food changes less than 1 degree F in 10 minutes

def smoker_callback(ch, method, properties, body):
    """
    Smoker_callback looks if the smoker temperature decreases by more than 15 degrees in 2.5 minutes.
    """
    try:
        data = body.decode()
        temp = float(data.split(":")[1].strip())
        smoker_temperature.append(temp)

        if len(smoker_temperature) == 5.0 and (smoker_temperature[0] - smoker_temperature[-1] >= SMOKER_ALERT):
            print(f"Smoker Alert! Temperature decreased by more than 15F in 2.5 minutes. Current Temp: {temp}F")
        else:
            print(f"[x] Received Smoker: {temp}F")
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except ValueError:
        print("Error converting temperature to float")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def FoodA_callback(ch, method, properties, body):
    """
    FoodA_callback looks if the food temperature changes less than 1F in 10 minutes.
    """
    try:
        data = body.decode()
        temp = float(data.split(":")[1].strip())
        food_a_temperature.append(temp)

        if len(food_a_temperature) == 20 and (abs(max(food_a_temperature) - min(food_a_temperature) <= FOOD_ALERT)):
            print(f"Food Stall Alert for Food A! Temperature changed less than 1F in 10 minutes. Current Temp: {temp}F")
        else:
            print(f"[x] Received FoodA: {temp}F")

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except ValueError:
        print("Error converting temperature to float")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def FoodB_callback(ch, method, properties, body):
    """
    FoodB_callback looks for a potential stall if the food temperature changes less than 1F in 10 minutes.
    """
    try:
        data = body.decode()
        temp = float(data.split(":")[1].strip())
   
        food_b_temperature.append(temp)

        if len(food_b_temperature) == 20 and (abs(max(food_b_temperature) - min(food_b_temperature) <= FOOD_ALERT)):
            print(f"Food Stall Alert for Food B! Temperature changed less than 1F in 10 minutes. Current Temp: {temp}F")
        else:
            print(f"[x] Received FoodB: {temp}F")

    # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except ValueError:
        print("Error while converting FoodB temperature to float")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main(hn: str,queue_name1,queue_name2,queue_name3):
    """
    Continuously listen for a message across 3 queues and processes message using the
    appropriate callback functions.
    """
    try:
        # Create a connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:    

        channel.queue_declare(queue=queue_name1, durable=True)
        channel.queue_declare(queue=queue_name2, durable=True)
        channel.queue_declare(queue=queue_name3, durable=True)

        # Set up the consumers for each queue
        channel.basic_consume(queue=queue_name1, on_message_callback=smoker_callback, auto_ack=False)
        channel.basic_consume(queue=queue_name2, on_message_callback=FoodA_callback, auto_ack=False)
        channel.basic_consume(queue=queue_name3, on_message_callback=FoodB_callback, auto_ack=False)

        # Inform the user that the consumer is ready to begin
        print(' [*] Waiting for messages. To exit press CTRL+C')
        
        # Start the consumers
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
        print("Error connecting to RabbitMQ server: ", e)
    except KeyboardInterrupt:
        print("User has stopped the process.")
    except Exception as e:
        print("An unexpected error occurred: ", e)
    finally:
        try:
            connection.close()
        except ValueError: 
            pass

if __name__ == "__main__":
    main("localhost","01-smoker","02-food-A","03-food-B")