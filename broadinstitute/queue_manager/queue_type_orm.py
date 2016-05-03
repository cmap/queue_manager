
class QueueTypeOrm(object):
    def __init__(self, id=None, name=None, description=None):
        self.id = id
        self.name = name
        self.description = description

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "id:{} name:{} description:{}".format(self.id, self.name, self.description)


def get_by_name(cursor, name):
    cursor.execute("select id, name, description from queue_type where name = ?", (name,))
    r = [x for x in cursor]
    if len(r) == 1:
        r = r[0]
        return QueueTypeOrm(id=r[0], name=r[1], description=r[2])
    elif len(r) == 0:
        return None
    else:
        raise Exception("queue_type_orm get_by_name found multiple queue_type entries for name:  {}".format(name))