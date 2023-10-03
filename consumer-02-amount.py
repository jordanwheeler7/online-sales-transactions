import pika
import sys

# Import function for sending email
from email_alert import createAndSendEmailAlert

# Configure logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# Initialize the original price as None
original_price = 0.00

# Define a callback function to be called when a message is received
def amount_callback(ch, method, properties, body):
    """ Define behavior on getting a message.
        This function will be called each time a message is received.
        The function must accept the four arguments shown here.
    """
    global original_price  # Use the global original_price variable

    # Decode the binary message body to a string
    logger.info(f" [x] Received {body.decode()}")

    # Extract the payment method from the message
    payment_method = body.decode()

    # Check if the payment method is "Store Card"
    if payment_method == "Store Card":
        if original_price is not None:
            # Apply a 10% discount
            discount = 0.10 * original_price
            new_price = original_price - discount

            # Check if the new price is greater than $450.00
            if new_price > float(425.00):
                logger.info(f"Original price was ${original_price:.2f} but you received a discount for using your store card, and the new price is ${new_price:.2f}. Sending email alert.")
                email_subject = "Store Card Purchase Alert"
                email_body = f"Purchase made with a store card. Original price was ${original_price:.2f} but the purchaser received a discount for using their store card, and the new price is ${new_price:.2f}."
                createAndSendEmailAlert(email_subject, email_body)
            else:
                logger.info(f"Original price was ${original_price:.2f} but you received a discount for using your store card, and the new price is ${new_price:.2f}")
        else:
            logger.warning("Original price is not available. Skipping discount calculation.")

    # If the message contains the price, update the original_price
    try:
        price = float(body.decode())
        original_price = price  # Update the original_price as a float
    except ValueError:
        logger.warning("Received a non-numeric value as the price. Skipping price update.")

    # Send Confirmation Report
    logger.info("[X] Payment Method Received and Processed.")
    # Delete Message from Queue after Processing
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Define a main function to run the program
def main(hn: str = "localhost", qn: str = "02-amount"):
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
        channel2 = connection.channel()

        # Use the channel to declare a durable queue
        # A durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # Messages will not be deleted until the consumer acknowledges
        channel2.queue_declare(queue=qn, durable=True)

        # The QoS level controls the number of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance.
        # prefetch_count = Per consumer limit of unacknowledged messages
        channel2.basic_qos(prefetch_count=1)

        # Configure the channel to listen on a specific queue,
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel2.basic_consume(queue=qn, on_message_callback=amount_callback, auto_ack=False)

        # Print a message to the console for the user
        logger.info(" [*] Ready for work. To exit, press CTRL+C")

        # Start consuming messages via the communication channel
        channel2.start_consuming()

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
    main("localhost", "02-amount")
