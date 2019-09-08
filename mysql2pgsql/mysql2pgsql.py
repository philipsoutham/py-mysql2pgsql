from __future__ import absolute_import

import codecs

from .lib import print_red
from .lib.config import Config
from .lib.converter import Converter
from .lib.errors import ConfigurationFileInitialized
from .lib.mysql_reader import MysqlReader
from .lib.postgres_db_writer import PostgresDbWriter
from .lib.postgres_file_writer import PostgresFileWriter


class Mysql2Pgsql(object):
    def __init__(self, options):
        self.run_options = options
        try:
            self.file_options = Config(options.file, True).options
        except ConfigurationFileInitialized as e:
            print_red(str(e))
            raise e

    def convert(self):
        reader = MysqlReader(self.file_options['mysql'])

        if self.file_options['destination']['file']:
            writer = PostgresFileWriter(self._get_file(self.file_options['destination']['file']),
                                        self.run_options.verbose)
        else:
            separation = self.file_options.get('separation', ' ')
            null = self.file_options.get('null', 'NULL')
            writer = PostgresDbWriter(self.file_options['destination']['postgres'], self.run_options.verbose,
                                      separation=separation, null=null)

        Converter(reader, writer, self.file_options, self.run_options.verbose).convert()

    def _get_file(self, file_path):
        return codecs.open(file_path, 'wb', 'utf-8')
