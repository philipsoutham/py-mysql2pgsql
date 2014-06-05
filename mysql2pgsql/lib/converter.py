from __future__ import absolute_import

from multiprocessing import Process
from threading import Thread
from Queue import Queue

from . import print_start_table


class Converter(object):
    def __init__(self, reader_class, reader_args, writer_class, writer_args,
                 file_options, num_procs=1, verbose=False):
        # We store the read/writer classes and args so that we can create
        # new instances for multiprocessing, via the get_reader and
        # get_writer methods.
        self.verbose = verbose
        self.reader_class = reader_class
        self.reader_args = reader_args
        self.reader = self.get_reader()
        self.writer_class = writer_class
        self.writer_args = writer_args
        self.writer = self.get_writer()
        self.file_options = file_options
        self.num_procs = num_procs
        self.exclude_tables = file_options.get('exclude_tables', [])
        self.only_tables = file_options.get('only_tables', [])
        self.supress_ddl = file_options.get('supress_ddl', None)
        self.supress_data = file_options.get('supress_data', None)
        self.force_truncate = file_options.get('force_truncate', None)

    def get_reader(self):
        return self.reader_class(*self.reader_args)

    def get_writer(self):
        return self.writer_class(*self.writer_args)

    def convert(self):
        if self.verbose:
            print_start_table('>>>>>>>>>> STARTING <<<<<<<<<<\n\n')

        tables = [t for t in (t for t in self.reader.tables if t.name not in self.exclude_tables) if not self.only_tables or t.name in self.only_tables]
        if self.only_tables:
            tables.sort(key=lambda t: self.only_tables.index(t.name))

        if not self.supress_ddl:
            if self.verbose:
                print_start_table('START CREATING TABLES')

            for table in tables:
                self.writer.write_table(table)

            if self.verbose:
                print_start_table('DONE CREATING TABLES')

        if self.force_truncate and self.supress_ddl:
            if self.verbose:
                print_start_table('START TRUNCATING TABLES')

            for table in tables:
                self.writer.truncate(table)

            if self.verbose:
                print_start_table('DONE TRUNCATING TABLES')

        if not self.supress_data:
            if self.verbose:
                print_start_table('START WRITING TABLE DATA')

            if self.num_procs == 1:
                # No parallel processing - process tables sequentially.
                for table in tables:
                    self.writer.write_contents(table, self.reader)
            else:
                # Parallel processing. Work is CPU bound so we need to
                # use multiprocessing, however the MySQL table objects
                # can't be pickled, so we're unable to easily build a
                # worker pool using multiprocessing, so we use threads
                # to manager the worker pool, with each worker thread
                # creating a new process for each table transferred.
                queue = Queue()
                for table in tables:
                    queue.put(table)
                for _ in range(self.num_procs):
                    queue.put(None)

                def worker():
                    while True:
                        writer = self.get_writer()
                        reader = self.get_reader()
                        table = queue.get()
                        if table is None:
                            return
                        proc = Process(target=writer.write_contents, args=(table, reader))
                        proc.start()
                        proc.join()

                threads = []
                for _ in range(self.num_procs):
                    threads.append(Thread(target=worker))
                    threads[-1].start()
                for thread in threads:
                    thread.join()

            if self.verbose:
                print_start_table('DONE WRITING TABLE DATA')

        if not self.supress_ddl:
            if self.verbose:
                print_start_table('START CREATING INDEXES, CONSTRAINTS, AND TRIGGERS')

            for table in tables:
                self.writer.write_indexes(table)

            for table in tables:
                self.writer.write_constraints(table)

            for table in tables:
                self.writer.write_triggers(table)

            if self.verbose:
                print_start_table('DONE CREATING INDEXES, CONSTRAINTS, AND TRIGGERS')

        if self.verbose:
            print_start_table('\n\n>>>>>>>>>> FINISHED <<<<<<<<<<')

        self.writer.close()
