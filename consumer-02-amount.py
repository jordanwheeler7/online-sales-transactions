"""
    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  

    Author: Jordan Wheeler
    Date: 2023-10-04
      
"""



import pika
import sys

# Import function for sending email
from email_alert import createAndSendEmailAlert

# Configure logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# Initialize the original price as None
original_price = None

# Define a callback function to be called when a message is received
def amount_callback(ch, method, properties, body):
    """ Define behavior on getting a message.
        This function will be called each time a message is received.
        The function must accept the four arguments shown here.
    """
    global original_price  # Declare original_price as a global variable

    # Decode the binary message body to a string
    message = body.decode()
    # Split Message
    message = message.split(",")
    message1 = message[0]
    message2 = message[1]
    formatted_message2 = "${:.2f}".format(float(message2))
    logger.info(f" [x] At {message1} a purchase has been made in the amount of {formatted_message2}")
    
    payment_amount_change = []
    try: 
            # Check for valid temperatures
        if message[1] != None and message[2] != '':
            # Convert to float
            payment_amount_change = round(float(message2), 2)
            # Check for valid timestamp
            payment_timestamp = message1
            
             
        # Check if payment method is store card and apply 10% discount
        payment_method = message[2]
        if payment_method == "Store Card":
            payment_amount_change = payment_amount_change * 0.9
            new_payment = payment_amount_change
            formatted_new_payment = "${:.02f}".format(new_payment)
            alert_message = True
        
        # Check if alert is true and payment is greater than $425.00
            if alert_message == True and new_payment >= 425.00:
                logger.warning(f"A Store Card has been used. The new price is {formatted_new_payment}.")
                # Create Email Parts
                email_subject = "Store Card Used"
                email_body = f"A Store Card has been used at {payment_timestamp}. The original price was {formatted_message2}. The new price is {formatted_new_payment}."
                createAndSendEmailAlert(email_subject, email_body)
                logger.info("Email Sent")
            
            logger.info(f"[X] Store Card Was Used. New price is {formatted_new_payment}.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    except Exception as e:
        logger.error("An Error Occurred While Processing Payment Amounts.")
        logger.error(f"The error says: {e}")

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
        channel.basic_consume(queue=qn, on_message_callback=amount_callback, auto_ack=False)

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
    main("localhost", "02-amount")
