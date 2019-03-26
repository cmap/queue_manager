import os
import sqs_queues.exceptions as qmExceptions


class CommanderTemplate(object):
    def __init__(self, base_path, espresso_path):
        self.base_path = base_path
        self.espresso_path = espresso_path
        self.plate = None
        self.command = None

    def __str__(self):
        return " ".join(["{}:{}".format(k, v) for (k, v) in self.__dict__.items()])

    def execute_command(self):
        os.system(self.command)

