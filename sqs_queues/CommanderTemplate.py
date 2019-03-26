import os
import sqs_queues.exceptions as qmExceptions


class CommanderTemplate(object):
    def __init__(self, cursor, base_path, espresso_path):
        self.base_path = base_path
        self.espresso_path = espresso_path
        self.cursor = cursor
        self.error = None
        self.plate = None
        self.command = None

    def __str__(self):
        return " ".join(["{}:{}".format(k, v) for (k, v) in self.__dict__.items()])

    def execute_command(self, ):
        try:
            os.system(self.command)
            self._post_build_success()
        except Exception as e:
            self.error = e
            self._post_build_failure()
            raise qmExceptions.FailureOccurredDuringProcessing(e)

    def _post_build_failure(self):
        pass

    def _post_build_success(self):
        pass