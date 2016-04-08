
class QueueTypeOrm(object):
    def __init__(self, id=None, name=None, description=None):
        self.id = id
        self.name = name
        self.description = description

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "id:{} name:{} description:{}".format(self.id, self.name, self.description)
