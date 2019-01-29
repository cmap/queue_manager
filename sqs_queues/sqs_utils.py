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
        m = Message(message, queue_url)
        messages.append(m)
    return messages


def consume_message_from_sqs_queue(message):
    response = SQS.delete_message(QueueUrl=message.queue_url,
                                  ReceiptHandle=message.receipt_handle)
    status = response['ResponseMetadata']['HTTPStatusCode']
    if status == 200:
        print "{}: Successfully consumed {} message from {} queue".format(status, message.receipt_handle, message.queue_url)

def clear_out_sqs_queue(queue_url):
    response = SQS.purge_queue(QueueUrl=queue_url)


class Message(object):
    def __init__(self, message, in_queue_url):
        self.machine_barcode = message['Body']
        self.receipt_handle = message['ReceiptHandle']
        self.current_queue_url = in_queue_url
    def _remove_from_current_queue(self):
        consume_message_from_sqs_queue(self)

    def pass_to_next_queue(self, queue_config):
        # NB: queue_config here is from ConfigParser.items(queue_name)
        self._remove_from_current_queue()
        send_message_to_sqs_queue(queue_config['queue_url'], self.machine_barcode, queue_config['tag'])

