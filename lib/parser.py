# Script converting an OSM changeset planet file into CSV and writing it to stdout.
# Performs a streamed conversion with low-memory usage so it can easily be run on a GitHub runner.
# Example usage:
# curl -L https://planet.openstreetmap.org/planet/changesets-latest.osm.bz2 | bzcat | (python parser.py --editor OpenStop) > output.csv
# Options:
# -e or --editor => Filter changesets by editor name

import xml.sax
import csv
import sys
import getopt

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

if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv[1:], 'e:', 'editor=')
    editor_filter = '';

    for opt, arg in opts:
        if opt in ('-e', '--editor'):
            editor_filter = arg

    csv_writer = csv.writer(sys.stdout)
    # Write CSV header
    csv_writer.writerow(Changeset.headerRow())
    # Filter and write rows
    def write(changeset):
        if changeset.created_by.startswith(editor_filter):
            csv_writer.writerow(changeset.toRow())
    xml.sax.parse(sys.stdin, ChangesetHandler(write))
