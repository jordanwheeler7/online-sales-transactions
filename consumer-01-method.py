import pika
import sys
import csv



# Configure logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# Define a callback function to be called when a message is received
def method_callback(ch, method, properties, body):
    """ Define behavior on getting a message.
        This function will be called each time a message is received.
        The function must accept the four arguments shown here.
    """
    # Decode the binary message body to a string
    logger.info(f" [x] Received {body.decode()}")

    # Extract the payment method from the message
    payment_method = body.decode()

    # Check if the payment method is "In Store Card"
    if payment_method == "In Store Card":
        # Apply a 10% discount
        logger.info("Applying a 10 percent discount for In Store Card purchase.")
        # Add your discount logic here

    # Send Confirmation Report
    logger.info("[X] Payment Method Received and Processed.")
    # Delete Message from Queue after Processing
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Define a main function to run the program
def main(hn: str = "localhost", qn: str = "task_queue"):
    """ Continuously listen for task messages on a named queue."""

    # When a statement can go wrong, use a try-except block
    try:
        # Try this code, if it works, keep going
        # Create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # Except, if there's an error, do this
    except Exception as e:
        print()
        logger.error("ERROR: Connection to RabbitMQ server failed.")
        logger.error(f"Verify the server is running on host={hn}.")
        logger.error(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # Use the connection to create a communication channel
        channel = connection.channel()

        # Use the channel to declare a durable queue
        # A durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # Messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn, durable=True)

        # The QoS level controls the number of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance.
        # prefetch_count = Per consumer limit of unacknowledged messages
        channel.basic_qos(prefetch_count=1)

        # Configure the channel to listen on a specific queue,
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume(queue=qn, on_message_callback=method_callback, auto_ack=False)

        # Print a message to the console for the user
        logger.info(" [*] Ready for work. To exit, press CTRL+C")

        # Start consuming messages via the communication channel
        channel.start_consuming()

    # Except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        logger.error("ERROR: Something went wrong.")
        logger.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        logger.info("User interrupted the continuous listening process.")
        sys.exit(0)
    finally:
        logger.info("\nClosing connection. Goodbye.\n")
        connection.close()

# Standard Python idiom to indicate the main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # Call the main function with the information needed
    main("localhost", "01-method")
