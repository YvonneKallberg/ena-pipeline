from ftplib import FTP, FTP_TLS
from hashlib import md5
from getpass import getpass
from xml.etree import ElementTree
from io import BytesIO, StringIO, SEEK_SET, SEEK_END
from pathlib import Path
import ssl
import io
import time

from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# https://github.com/amnong/easywebdav/blob/master/easywebdav/client.py

#ftp.voidcmd("XMD5 " + filename)
#ftp.voidcmd("MD5 " + filename)


class MyFTP_TLS(FTP_TLS):
    """Explicit FTPS, with shared TLS session"""
    def ntransfercmd(self, cmd, rest=None):
        conn, size = FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(conn,
                                            server_hostname=self.host,
                                            session=self.sock.session)  # this is the fix
        return conn, size

def iterable_to_stream(iterable, buffer_size=io.DEFAULT_BUFFER_SIZE):
    """
    Lets you use an iterable (e.g. a generator) that yields bytestrings as a read-only
    input stream.

    The stream implements Python 3's newer I/O API (available in Python 2's io module).
    For efficiency, the stream is buffered.
    """
    class IterStream(io.RawIOBase):
        def __init__(self):
            self.leftover = None
        def readable(self):
            return True
        def readinto(self, b):
            try:
                l = len(b)  # We're supposed to return at most this much
                chunk = self.leftover or next(iterable)
                output, self.leftover = chunk[:l], chunk[l:]
                b[:len(output)] = output
                return len(output)
            except StopIteration:
                return 0    # indicate EOF
    return io.BufferedReader(IterStream(), buffer_size=buffer_size)


class NextcloudNPCReader:

    def __init__(self, webdav_host, webdav_user, webdav_pass, webdav_dir):
        self.webdav_host = webdav_host
        self.webdav_user = webdav_user
        self.webdav_pass = webdav_pass
        self.webdav_dir = webdav_dir
        self.session = Session()
        retry = Retry(connect=10, backoff_factor=3)
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('https://', adapter)

    def ls(self, webdav_dir):

        list_response = self.session.request(
            'PROPFIND',
            f'{self.webdav_host}{webdav_dir}',
            auth=(self.webdav_user, self.webdav_pass)
        )
        response_xml = ElementTree.fromstring(list_response.content)
        file_list = [
            e.text
            for e in response_xml.findall('.//d:href', namespaces={'d':'DAV:'})
        ]
        return file_list

    def ls_size(self, webdav_dir):

        list_response = self.session.request(
            'PROPFIND',
            f'{self.webdav_host}{webdav_dir}',
            auth=(self.webdav_user, self.webdav_pass)
        )
        response_xml = ElementTree.fromstring(list_response.content)
        file_list = [
            {
                'filename': Path(e.find('.//d:href', namespaces={'d':'DAV:'}).text).name,
                'size': {i:int(size.text) for i, size in enumerate(e.findall('.//d:propstat/d:prop/d:getcontentlength', namespaces={'d':'DAV:'}))}.get(0,0),
            }
            for e in response_xml.findall('.//d:response', namespaces={'d':'DAV:'})
        ]
        return file_list

    def open(self, webdav_path, chunk_size=1024*1024, buffer_factor=1000):
        get_response = self.session.get(
            f'{self.webdav_host}{webdav_path}',
            auth=(self.webdav_user, self.webdav_pass),
            stream=True
        )

        #bytes_io = ResponseStream(get_response.iter_content(chunk_size=8192))
        #return bytes_io
        return iterable_to_stream(get_response.iter_content(chunk_size=chunk_size), buffer_factor * chunk_size)

class ENAFTPWriter:

    def __init__(self, ftp_host, ftp_user, ftp_pass, ftp_dir):
        self.ftp_host = ftp_host
        self.ftp_user = ftp_user
        self.ftp_pass = ftp_pass
        self.ftp_dir = ftp_dir
        self.ftp = None

    def establish_connection(self):
        #context = ssl.SSLContext(ssl.PROTOCOL_TLS)

        if self.ftp is not None:
            try:
                self.ftp.voidcmd("NOOP")
            except:
                self.ftp = None

        if self.ftp is None:
            ftp = MyFTP_TLS(timeout=10)
            ftp.connect(self.ftp_host, port=21)
            ftp.auth()
            ftp.login(self.ftp_user, self.ftp_pass)
            ftp.prot_p()
            
            #ftp.set_pasv(False)
            ftp.cwd(self.ftp_dir)
            self.ftp = ftp

    def upload(self, filename, file, callback=lambda x: None, blocksize=1020*1024):
        calculated_md5 = md5()

        def update_md5(chunk):
            calculated_md5.update(chunk)
            return callback(chunk)
        
        self.establish_connection()
        # APPE
        self.ftp.storbinary(f"STOR {filename}", file, blocksize=blocksize, callback=update_md5)

        return calculated_md5.hexdigest()

    def delete(self, filename):
        self.establish_connection()
        self.ftp.delete(filename)

    def ls(self):
        self.establish_connection()
        return self.ftp.nlst()

    def ls_size(self):
        self.establish_connection()
        lines = []
        self.ftp.dir(lines.append)
        return [
            {
                'filename': line[-1],
                'size': int(line[4]),
            }
            for line in [line.split() for line in lines]
        ]

    def size(self, filename):
        self.establish_connection()
        return self.ftp.size(filename)


def transfer(npc, ena, webdav_dir):

    file_list = npc.ls(webdav_dir)
    ftp_files = ena.ls()

    print(f'{len(file_list)} files in path "{webdav_dir}"')
    md5_mapping = {
        file[:-4]:file
        for file in file_list
        if file.endswith('.md5')
    }
    fasta_files = [
        file
        for file in file_list
        if file.endswith('.fq.gz')
    ]
    

    for file in fasta_files:
        filename = Path(file).name
        if filename in ftp_files:
            print(f'Skipping {filename}')
            continue
        try:
            print(f'\r'+(' '*120), end='')
            print(f'\rProcessing {filename}: Reading MD5...', end='')

            md5_file = npc.open(md5_mapping[file])
            md5_file_contents = BytesIO()
            ena.upload(f'{filename}.md5', md5_file, callback=lambda c: md5_file_contents.write(c))
            md5_hash = md5_file_contents.getvalue().decode('utf-8')[:32]

            print(f'\r'+(' '*120), end='')
            print(f'\rProcessing {filename}: Reading MD5...{md5_hash}', end='')

            upload_status = {
                'bytes': 0,
                'counter': 0,
            }
            def status(chunk):
                upload_status['bytes'] += len(chunk)
                if upload_status['counter'] % (1024*1024*10) == 0:
                    print(f'\rProcessing {filename}: Transferred {int(upload_status["bytes"]/1024/1024)} MB', end='')

            print(f'\r'+(' '*120), end='')
            print(f'\rProcessing {filename}: Opening...', end='')

            fastq_file = npc.open(file, chunk_size=10*1024*1024, buffer_factor=10)

            print(f'\r'+(' '*120), end='')
            print(f'\rProcessing {filename}: Transferred {int(upload_status["bytes"]/1024/1024)} MB', end='')
            
            ftp_hash = ena.upload(filename, fastq_file, callback=status, blocksize=10*1024*1024)

            print(f'\r'+(' '*120), end='')
            print(f'\rProcessing {filename}: Fisnished {int(upload_status["bytes"]/1024/1024)} MB', end='')
        except (KeyboardInterrupt) as e:
            print(f'\r'+(' '*120), end='')
            print(f'\rCancelled: deleting {filename}')
            ena.delete(filename)
            print(f'\r'+(' '*120), end='')
            print(f'\rCancelled: {filename}')
            break
        except (Exception) as e:
            if ena.size(filename):
                print(f'\rCancelled: deleting {filename}')
                ena.delete(filename)
            print(f'\r'+(' '*120), end='')
            print(f'\rFailed: {filename}', e)
            continue

        if md5_hash == ftp_hash:
            print(f'\r'+(' '*120), end='')
            print(f'\rSuccessfully uploaded {filename}: {md5_hash}')
        else:
            print(f'\r'+(' '*120), end='')
            print(f'\r! Uploaded {filename} with invalid hash: {md5_hash} (given) ≠ {ftp_hash} (ftp)')


