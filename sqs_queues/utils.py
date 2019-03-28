import os
import shutil

def deprecate_directory(base_directory, target):
    deprecated_directory = os.path.join(base_directory, "deprecated")
    deprecated_target = os.path.join(deprecated_directory, os.path.basename(target))

    if not os.path.exists(deprecated_directory):
        os.mkdir(deprecated_directory)

    if os.path.exists(deprecated_target):
        shutil.rmtree(deprecated_target)

    shutil.move(target, deprecated_directory)


