# Script to download the OSM changeset planet file and convert its data into CSV.
# It is designed for low-storage and low-memory usage so it can easily be run on a GitHub runner.
# -s or --source => Specify the source URL of the compressed changeset planet file
# -e or --editor => Filter changesets by editor name

import xml.sax
import csv
import sys
import getopt
import requests
import threading
import queue
import bz2
from tqdm import tqdm

class Changeset():
    id = 0
    created_at = ''
    closed_at = ''
    num_changes = 0
    uid = 0
    min_lat = 0
    max_lat = 0
    min_lon = 0
    max_lon = 0
    comment = ''
    created_by = ''
    locale = ''

    @staticmethod
    def headerRow():
        return [
            'id', 'created_at', 'closed_at', 'num_changes', 'uid',
            'min_lat', 'max_lat', 'min_lon', 'max_lon',
            'comment', 'created_by', 'locale'
        ]

    def toRow(self):
        return [
            self.id, self.created_at, self.closed_at, self.num_changes, self.uid,
            self.min_lat, self.max_lat, self.min_lon, self.max_lon,
            self.comment, self.created_by, self.locale,
        ]


class ChangesetHandler(xml.sax.ContentHandler):
    def __init__(self, callback):
        self.current_changeset = None
        self.callback = callback

    def startElement(self, name, attrs):
        if name == 'changeset':
            self.current_changeset = Changeset()
            self.current_changeset.id = attrs.get('id')
            self.current_changeset.created_at = attrs.get('created_at')
            self.current_changeset.closed_at = attrs.get('closed_at')
            self.current_changeset.num_changes = attrs.get('num_changes')
            self.current_changeset.uid = attrs.get('uid')
            self.current_changeset.min_lat = attrs.get('min_lat')
            self.current_changeset.max_lat = attrs.get('max_lat')
            self.current_changeset.min_lon = attrs.get('min_lon')
            self.current_changeset.max_lon = attrs.get('max_lon')
        elif name == 'tag' and self.current_changeset:
            match attrs.get('k'):
              case 'comment':
                self.current_changeset.comment = attrs.get('v')
              case 'created_by':
                self.current_changeset.created_by = attrs.get('v')
              case 'locale':
                self.current_changeset.locale = attrs.get('v')

    def endElement(self, name):
        if name == 'changeset':
            self.callback(self.current_changeset)


def downloader(url, q):
    '''Thread function to download file and populate the queue with chunks'''
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            # Only required for progress bar
            total = int(response.headers.get('content-length', 0))
            with tqdm(total=total, unit='B', unit_scale=True) as progress_bar:
                for chunk in response.iter_content(chunk_size=1048576): # 1MB chunks
                    progress_bar.update(len(chunk))
                    # Blocks when the queue is full
                    q.put(chunk)
    except queue.ShutDown:
        return
    except Exception as e:
        print(f'Download error: {e}')
    finally:
        q.shutdown()


def decompressor(q_in, q_out):
    '''Thread function to decompress the chunks from the queue'''
    decompressor = bz2.BZ2Decompressor()

    while True:
        try:
            # Blocks when the queue is empty
            chunk = q_in.get()
            content = decompressor.decompress(chunk)
            # Required, see last section of https://stackoverflow.com/questions/49569394/using-bz2-bz2decompressor/49584544#49584544
            while decompressor.eof:
                remaining_data = decompressor.unused_data
                decompressor = bz2.BZ2Decompressor()
                content += decompressor.decompress(remaining_data)
            # This blocks when the queue is full
            q_out.put(content)
            q_in.task_done()
        except queue.ShutDown:
            q_in.shutdown()
            q_out.shutdown()
            return
        except Exception as e:
            print(f'Decompression error: {e}')
            q_in.shutdown(immediate=True)
            q_out.shutdown(immediate=True)
            return


def parser(q, editor_filter):
    '''Thread function to parse the xml chunks from the queue'''
    with open('output.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        # Write CSV header
        csv_writer.writerow(Changeset.headerRow())

        parser = xml.sax.make_parser()
        def write(changeset):
            if changeset.created_by.startswith(editor_filter):
                csv_writer.writerow(changeset.toRow())
        parser.setContentHandler(ChangesetHandler(write))

        while True:
            try:
                # Blocks when the queue is empty
                content = q.get()
                parser.feed(content)
                q.task_done()
            except queue.ShutDown:
                break
            except Exception as e:
                print(f'Parsing error: {e}')
                q.shutdown(immediate=True)
                return

        parser.close()


if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv[1:], 's:e:', 'source=editor=')
    source_url = 'https://planet.openstreetmap.org/planet/changesets-latest.osm.bz2'
    editor_filter = '';
    queue_size = 10 # Number of chunks to buffer

    for opt, arg in opts:
        if opt in ('-s', '--source'):
            source_url = arg
        elif opt in ('-e', '--editor'):
            editor_filter = arg

    # Create communication queues
    chunk_queue = queue.Queue(maxsize=queue_size)
    content_queue = queue.Queue(maxsize=queue_size)

    # Create and start threads
    download_thread = threading.Thread(
        target=downloader,
        args=(source_url, chunk_queue)
    )
    decompress_thread = threading.Thread(
        target=decompressor,
        args=(chunk_queue,content_queue)
    )
    parse_thread = threading.Thread(
        target=parser,
        args=(content_queue,editor_filter)
    )

    download_thread.start()
    decompress_thread.start()
    parse_thread.start()

    # Wait for all threads to complete
    download_thread.join()
    decompress_thread.join()
    parse_thread.join()

    print('Done')
