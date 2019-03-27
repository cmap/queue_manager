import boto3

def get_queue_url_from_name(queue_name):
    #todo: may require AWS configuration
    SQS = boto3.client('sqs')

    response = SQS.get_queue_url(QueueName=queue_name)
    if response:
        print response


def send_message_to_sqs_queue(queue_url, message_body, tag):
    """
    NB: MessageGroupId is required for FIFO queues, allows interleaving of ordered streams,
    here unique identifiers should help prevent duplicate messages

    :param queue_url:
    :param message_body:
    :param tag: (string) appended queue identifier for DLQ
    :return:
    """
    SQS = boto3.client('sqs')

    message = str(message_body)

    deduplicator = message + "_" + tag
    response = SQS.send_message(QueueUrl=queue_url,
                                MessageBody=message,
                                MessageGroupId=message,
                                MessageDeduplicationId=deduplicator)

    status = response['ResponseMetadata']['HTTPStatusCode']
    if status == 200:
        print "{}: Successfully sent {} message to {}".format(status, message_body, queue_url.rsplit("/", 1)[1])


def receive_many_messages_from_sqs_queue(queue_url, atmost):
    SQS = boto3.client('sqs')
    if atmost == 1:
        return receive_message_from_sqs_queue(queue_url)


    response = SQS.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=atmost)

    if response["Messages"]:
        status = response['ResponseMetadata']['HTTPStatusCode']
        print "{}: Received {} messages from {} queue".format(status, len(response['Messages']), queue_url.rsplit("/",1)[1])

        messages = []
        for message in response['Messages']:
            m = Message(message, queue_url)
            messages.append(m)
        return messages

    else :
        print "No messages to receive in {} queue".format(queue_url.rsplit("/",1)[1])
        return None

def receive_message_from_sqs_queue(queue_url):
    SQS = boto3.client('sqs')

    response = SQS.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)

    if response["Messages"]:
        m = Message(response['Messages'][0], queue_url)
        return m
    print "No messages to receive in {} queue".format(queue_url.rsplit("/", 1)[1])
    return None

def consume_message_from_sqs_queue(message):
    SQS = boto3.client('sqs')

    response = SQS.delete_message(QueueUrl=message.current_queue_url,
                                  ReceiptHandle=message.receipt_handle)
    status = response['ResponseMetadata']['HTTPStatusCode']
    if status == 200:
        print "{}: Successfully consumed {} message from {} queue".format(status, message.machine_barcode, message.current_queue_url)


def clear_out_sqs_queue(queue_url):
    SQS = boto3.client('sqs')
    response = SQS.purge_queue(QueueUrl=queue_url)


class Message(object):
    def __init__(self, message, current_queue):
        self.machine_barcode = message['Body']
        self.receipt_handle = message['ReceiptHandle']
        self.current_queue_url = current_queue

    def __repr__(self):
        key_list = [x for x in self.__dict__.keys()]
        key_list.sort()
        ret_list = ["{}:{}".format(x, self.__dict__[x]) for x in key_list]

        return " ".join(ret_list)

    def __str__(self):
        return self.__repr__()

    def _remove_from_current_queue(self):
        consume_message_from_sqs_queue(self)

    def pass_to_next_queue(self, next_queue_config):
        # NB: queue_config here is from ConfigParser.items(queue_name)
        self._remove_from_current_queue()
        send_message_to_sqs_queue(next_queue_config['queue_url'], self.machine_barcode, next_queue_config['tag'])