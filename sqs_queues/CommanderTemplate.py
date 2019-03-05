import subprocess
import sqs_queues.exceptions as qmExceptions


class CommanderTemplate(object):
    def __init__(self, base_path, espresso_path):
        self.base_path = base_path
        self.espresso_path = espresso_path
        self.plate = None
        self.command = None

    def execute_command(self):
        try:
            subprocess.check_call(self.command)
        except subprocess.CalledProcessError as e:
            raise qmExceptions.FailureOccurredDuringProcessing(e)

