import boto3


SQS = boto3.client('sqs')

def get_queue_url_from_name(queue_name):
    #todo: may require AWS configuration
    response = SQS.get_queue_url(QueueName=queue_name)
    if response:
        print response

def send_message_to_sqs_queue(queue_url, message, tag):
    """
    NB: MessageGroupId is required for FIFO queues, allows interleaving of ordered streams,
    here unique identifiers should help prevent duplicate messages

    :param queue_url:
    :param message:
    :param tag: (string) appended queue identifier for DLQ
    :return:
    """
    deduplicator = message + "_" + tag
    response = SQS.send_message(QueueUrl=queue_url,
                                MessageBody=message,
                                MessageGroupId=message,
                                MessageDeduplicationId=deduplicator)

    status = response['ResponseMetadata']['HTTPStatusCode']
    if status == 200:
        print "{}: Successfully sent {} message to {}".format(status, message, queue_url.rsplit("/",1)[1])


def receive_messages_from_sqs_queue(queue_url):
    response = SQS.receive_message(QueueUrl=queue_url)
    status = response['ResponseMetadata']['HTTPStatusCode']

    print "{}: Received {} messages from {} queue".format(status, len(response['Messages']), queue_url.rsplit("/",1)[1])

    messages = []
    for message in response['Messages']:
        m = {
            "machine_barcode": message['Body'],
            "receipt_handle": message['ReceiptHandle']
        }
        messages.append(m)
    return messages


def consume_message_from_sqs_queue(queue_url, receipt_handle):

    response = SQS.delete_message(QueueUrl=queue_url,
                                  ReceiptHandle=receipt_handle)
    status = response['ResponseMetadata']['HTTPStatusCode']
    if status == 200:
        print "{}: Successfully consumed {} message from {} queue".format(status, receipt_handle, queue_url)

def clear_out_sqs_queue(queue_url):
    response = SQS.purge_queue(QueueUrl=queue_url)


class Message(object):
    def __init__(self, message, in_queue_name):
        self.machine_barcode = message['Body']
        self.receipt_handle = message['ReceiptHandle']
        self.current_queue = in_queue_name


messages = receive_messages_from_sqs_queue('https://sqs.us-east-1.amazonaws.com/207675869076/yeezy.fifo')
for message in messages:
    print message.machine_barcode