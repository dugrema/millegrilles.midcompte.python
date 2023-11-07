import os
import pysftp
from urllib.parse import urlparse
from pysftp import CnOpts
from stat import S_ISDIR


class Sftp:
    def __init__(self, hostname, username, private_key, port=22):
        """Constructor Method"""
        # Set connection object to None (initial value)
        self.connection = None
        self.hostname = hostname
        self.username = username
        #self.password = password
        self.private_key = private_key
        self.port = port

    def connect(self):
        """Connects to the sftp server and returns the sftp connection object"""

        try:
            # Get the sftp connection object
            cnopts = CnOpts(knownhosts='~')
            cnopts.hostkeys = None
            # cnopts.hostkeys.load('sftpserver.pub')

            self.connection = pysftp.Connection(
                host=self.hostname,
                username=self.username,
                # password=self.password,
                private_key=self.private_key,
                port=self.port,
                cnopts=cnopts,
            )
        except Exception as err:
            raise Exception(err)
        finally:
            print(f"Connected to {self.hostname} as {self.username}.")

    def disconnect(self):
        """Closes the sftp connection"""
        self.connection.close()
        print(f"Disconnected from host {self.hostname}")

    def listdir(self, remote_path):
        """lists all the files and directories in the specified path and returns them"""
        for obj in self.connection.listdir(remote_path):
            yield obj

    def listdir_attr(self, remote_path):
        """lists all the files and directories (with their attributes) in the specified path and returns them"""
        for attr in self.connection.listdir_attr(remote_path):
            yield attr

    def is_dir(self, file_stat):
        return S_ISDIR(file_stat.st_mode)

    def upload(self, source_local_path, remote_path):
        """
        Uploads the source files from local to the sftp server.
        """

        try:
            print(
                f"uploading to {self.hostname} as {self.username} [(remote path: {remote_path});(source local path: {source_local_path})]"
            )

            # Download file from SFTP
            self.connection.put(source_local_path, remote_path)
            print("upload completed")

        except Exception as err:
            raise Exception(err)

    def download(self, remote_path, target_local_path):
        """
        Downloads the file from remote sftp server to local.
        Also, by default extracts the file to the specified target_local_path
        """

        try:
            print(
                f"downloading from {self.hostname} as {self.username} [(remote path : {remote_path});(local path: {target_local_path})]"
            )

            # Create the target directory if it does not exist
            path, _ = os.path.split(target_local_path)
            if not os.path.isdir(path):
                try:
                    os.makedirs(path)
                except Exception as err:
                    raise Exception(err)

            # Download from remote sftp server to local
            self.connection.get(remote_path, target_local_path)
            print("download completed")

        except Exception as err:
            raise Exception(err)

    def parcours_fichiers_recursif(self, remote_path: str, filtre=None):
        listing = self.listdir_attr(remote_path)
        for item in listing:
            if filtre:
                if not filtre(item):
                    continue

            if self.is_dir(item):
                subpath = os.path.join(remote_path, item.filename)
                for sub_item in self.parcours_fichiers_recursif(subpath):
                    yield sub_item
            else:
                yield {"file": item, "directory": remote_path}


if __name__ == "__main__":
    sftp = Sftp(
        hostname='thinkcentre1.maple.maceroc.com',
        username='mgfichiers',
        private_key='/home/mathieu/tmp/passwd.fichiers_ed25519.txt'
    )

    # Connect to SFTP
    sftp.connect()

    # Lists files with attributes of SFTP
    path = "/mnt/tas1500/mgfichiers"
    print(f"List of files with attributes at location {path}:")
    # for file in sftp.listdir_attr(path):
    #     est_dir = sftp.is_dir(file)
    #     print(file.filename, file.st_mode, file.st_size, file.st_atime, file.st_mtime)

    print("***")
    for file in sftp.parcours_fichiers_recursif(path):
        item = file['file']
        dir = file['directory']
        print("Fichier %s/%s (%d bytes)" % (dir, item.filename, item.st_size))
    print("***")

    # Upload files to SFTP location from local
    local_path = "/home/mathieu/tmp/test2/fernande.jpg"
    remote_path = "/mnt/tas1500/mgfichiers/test2/fernande.jpg"
    sftp.upload(local_path, remote_path)

    # Lists files of SFTP location after upload
    print(f"List of files at location {path}:")
    print([f for f in sftp.listdir(path)])

    # Download files from SFTP
    # sftp.download(
    #     remote_path, os.path.join(remote_path, local_path + '.backup')
    # )

    # Disconnect from SFTP
    sftp.disconnect()
