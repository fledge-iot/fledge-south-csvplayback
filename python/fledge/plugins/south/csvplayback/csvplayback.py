# -*- coding: utf-8 -*-

# FLEDGE_BEGIN
# See: http://fledge-iot.readthedocs.io/
# FLEDGE_END

""" Module for CSV playback poll plugin using pandas """

import copy
import logging
import os
from threading import Event
from threading import Thread, Condition
import datetime
import time
import glob

import pandas as pd
import numpy as np

import async_ingest
from fledge.common import logger
from fledge.plugins.common import utils

__author__ = "Rajesh Kumar, Deepanshu Yadav, Douglas Orr"
__copyright__ = "Copyright (c) 2020 Dianomic Systems Inc."
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

# SETTINGS
POLL_MODE=True  # -> poll(t) or async(f)
TIME_IT = True  # async timing information

# GLOBAL VARIABLES DECLARATION
_FLEDGE_ROOT = os.getenv("FLEDGE_ROOT", default='/usr/local/fledge')
_FLEDGE_DATA = os.path.expanduser(_FLEDGE_ROOT + '/data')
_FLEDGE_DATA_PREFIX = "FLEDGE_DATA"

_LOGGER = logger.setup(__name__, level=logging.DEBUG)

plugin_mode = {True: 'poll', False: 'async'}[POLL_MODE]
producer = None  # A Producer object to read data from csv file
consumer = None  # A Consumer object to ingest data to database
condition = None  # A conditional variable for mutual exclusivity among consumer and producer
wait_event = Event()  # A variable to control the rate of ingest into database and indicate shutdown
c_callback = None
c_ingest_ref = None
_sentinel = object()  # Indicates the file has been read
readingsQueue = None  # Queue/ buffer to store readings.

reader = None  # object holding state of current csv dataframe

_DEFAULT_CONFIG = {
    'plugin': {
        'description': 'Reads data from csv file through pandas API and ingests into database.',
        'type': 'string',
        'default': 'csvplayback',
        'readonly': 'true'
    },
    'assetName': {
        'description': 'Name of Asset',
        'type': 'string',
        'default': "vibration",
        'displayName': 'Asset name',
        'order': '1'
    },
    'csvDirName': {
        'description': 'The directory where CSV file exists. Default is '
                       'FLEDGE_DATA or FLEDGE_ROOT/data',
        'type': 'string',
        'default': 'FLEDGE_DATA',
        'displayName': 'CSV directory name',
        'order': '2'
    },
    'csvFileName': {
        'description': 'CSV file name or pattern to search inside directory. Not necessarily an '
                       'exact file name.',
        'type': 'string',
        'default': '',
        'displayName': 'CSV file pattern',
        'order': '3'
    },
    'headerMethod': {
        'description': 'Method for processing the header.',
        'type': 'enumeration',
        'default': 'do_not_skip',
        'options': ['skip_rows', 'pass_in_datapoint', 'do_not_skip'],
        'displayName': 'Header processing method',
        'order': '4'
    },
    'dataPointForCombine': {
        'description': 'If header method is pass_in_datapoint then it is the datapoint name '
                       'where the given number of rows will get combined.',
        'type': 'string',
        'default': 'metadata',
        'displayName': 'Data point for header rows',
        'validity': "headerMethod == \"pass_in_datapoint\"",
        'order': '5'
    },
    'noOfRows': {
        'description': 'No. of rows to skip or combine to single value.',
        'type': 'integer',
        'default': '1',
        'displayName': 'Number of rows to skip or pass in datapoint',
        'validity': "headerMethod == \"skip_rows\" || headerMethod == \"pass_in_datapoint\"",
        'minimum': '1',
        'order': '6'
    },
    'variableCols': {
        'description': 'Are variable number of values present in every row?',
        'type': 'boolean',
        'default': 'false',
        'displayName': 'Dynamic columns',
        'order': '7'
    },
    'columnMethod': {
        'description': 'Method for getting the column names.',
        'type': 'enumeration',
        'default': 'pick_from_file',
        'options': ['explicit', 'pick_from_file'],
        'validity': "variableCols == \"false\"",
        'displayName': 'Column processing method',
        'order': '8'
    },
    'autoGeneratePrefix': {
        'description': 'The prefix for auto generation of columns if variable Columns is true.',
        'type': 'string',
        'default': 'column',
        'displayName': 'Auto generate prefix',
        'validity': "variableCols == \"true\"",
        'order': '9'
    },
    'useColumns': {
        'description': 'Comma separated list of column names [:types] for selection / name / type override; '
                       'if column method is explicit.',
        'type': 'string',
        'default': '',
        'displayName': 'Column names and data types for explicit',
        'validity': "columnMethod == \"explicit\" && variableCols == \"false\"",
        'order': '10'
    },
    'rowIndexForColumnNames': {
        'description': 'The row index for picking column names if column method is '
                       'pick_from_file',
        'type': 'integer',
        'default': '0',
        'minimum': '0',
        'displayName': 'Row index for column names',
        'validity': "columnMethod == \"pick_from_file\" && variableCols == \"false\"",
        'order': '11'
    },
    'ingestMode': {
        'description': 'Mode of data ingest - burst/continuous',
        'type': 'enumeration',
        'default': 'burst',
        'options': ['continuous', 'burst'],
        'displayName': 'Ingest mode',
        'order': '12'
    },
    'sampleRate': {
        'description': 'No. of readings per sec.',
        'type': 'integer',
        'default': '8000',
        'displayName': 'Sample rate',
        'minimum': '1',
        'maximum': '1000000',
        'validity': "variableCols == \"false\"",
        'order': '13'
    },
    'burstInterval': {
        'description': 'Time interval between consecutive bursts in milliseconds; mandatory for "burst" mode',
        'type': 'integer',
        'default': '1000',
        'validity': "ingestMode == \"burst\"",
        'displayName': 'Burst interval (ms)',
        'minimum': '1',
        'order': '14'
    },
    'timestampStyle': {
        'description': 'Select "continuous" mode asset timestamp processing style.',
        'type': 'enumeration',
        'default': 'current time',
        'validity': "variableCols == \"false\"",
        'options': ['current time', 'copy csv value', 'move csv value', 'use csv sample delta'],
        'displayName': 'Timestamp processing mode',
        'order': '15'
    },
    'timestampCol': {
        'description': 'Timestamp header column, mandatory for "move/copy csv value" or'
                       ' "use csv sample delta" timestamp style',
        'type': 'string',
        'default': '',
        'validity': "timestampStyle == \"copy csv value\" || timestampStyle == \"move csv value\" ||  "
                    "timestampStyle == \"use csv sample delta\"",
        'displayName': 'Timestamp column name',
        'order': '16'
    },
    'timestampFormat': {
        'description': 'Timestamp format in File; mandatory when timestamp column is used',
        'type': 'string',
        'default': '%Y-%m-%d %H:%M:%S.%f%z',
        'validity': "timestampStyle == \"copy csv value\" || timestampStyle == \"move csv value\" ||  "
                    "timestampStyle == \"use csv sample delta\"",
        'displayName': 'Timestamp format',
        'order': '17'
    },
    'ignoreNaN': {
        'description': 'Ignore the NaN values or report error. NaN values occur due to whitespaces'
                       ' and missing values in CSV file. Default action is ignore. If report is selected'
                       'and error is found delete the south service and try again with a clean CSV file.',
        'type': 'enumeration',
        'default': 'ignore',
        'options': ['ignore', 'report'],
        'displayName': 'Ignore or report for NaN',
        'validity': "variableCols == \"false\"",
        'order': '18'
    },
    'postProcessMethod': {
        'description': 'Action to perform when file being played gets finished.',
        'type': 'enumeration',
        'options': ['continue_playing', 'delete', 'rename'],
        'default': 'continue_playing',
        'displayName': 'Post process method',
        'order': '19'
    },
    'suffixName': {
        'description': 'Suffix Name to append to file if Post Process Method is '
                       'rename',
        'type': 'string',
        'default': '.tmp',
        'displayName': 'Suffix name',
        'validity': "postProcessMethod == \"rename\"",
        'order': '20'
    },

}


def plugin_info():
    """ Returns information about the plugin.
    Args:
    Returns:
        dict: plugin information
    Raises:
    """
    global plugin_mode

    return {
        'name': "CSV Playback",
        'version': '1.9.1',
        'mode': plugin_mode,
        'type': 'south',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }


def plugin_init(config):
    """ Initialise the plugin.
    Args:
        config: JSON configuration document for the South plugin configuration category
    Returns:
        handle: JSON object to be used in future calls to the plugin
    Raises:
    """

    handle = copy.deepcopy(config)

    global mode
    handle['mode'] = {'value': plugin_mode}

    try:
        errors = False

        if int(handle['sampleRate']['value']) < 1 or int(handle['sampleRate']['value']) > 1000000:
            _LOGGER.error("sampleRate should be in range 1-1000000")
            errors = True
        if int(handle['burstInterval']['value']) < 1:
            _LOGGER.error("burstInterval should not be less than 1")
            errors = True
        if handle['ingestMode']['value'] not in ['burst', 'continuous']:
            _LOGGER.error("ingestMode should be one of ('burst', 'continuous')")
            errors = True
        if handle['timestampStyle']['value'] in ['copy csv value', 'move csv value', 'use csv sample delta'] and \
                (handle['timestampCol']['value'] == '' or handle['timestampFormat']['value'] == ''):
            _LOGGER.error("timestamp Column (of csv File) and timestamp Format must be specified ")
            errors = True
        if (handle['timestampStyle']['value'] != 'current time') and (handle['ingestMode']['value'] == 'burst'):
            _LOGGER.error("Historic and delta timestamps are only used in ""continuous"" mode")
            errors = True
        if errors:
            raise RuntimeError("{} plugin_init failed".format(__name__))

        # calculate period, burst size, and chunk size
        try:
            if handle['ingestMode']['value'] == 'burst':
                burst_interval = int(handle['burstInterval']['value'])
                # chunk up a "burst's" worth of samples
                period = round(burst_interval / 1000.0, len(str(burst_interval)) + 1)
                if handle['variableCols']['value'] == 'false':
                    recs = int(period * int(handle['sampleRate']['value']))
                else:
                    recs = 1
            else:
                # chunk up a second's worth of samples
                if handle['variableCols']['value'] == 'false':
                    recs = int(handle['sampleRate']['value'])
                else:
                    recs = 1

                period = round(1.0 / recs, len(str(recs)) + 1)
                _LOGGER.info("recs is {} and period is {}".format(recs, period))
        except ZeroDivisionError:
            _LOGGER.warning('sampleRate must be greater than 0, defaulting to 1')
            period = 1.0

        handle['period'] = {'value': period}
        handle['chunkSize'] = {'value': recs}

        # initialize the object that maintains csv state
        global reader
        reader = CSVReader(handle)

    except KeyError:
        raise
    except EOFError:
        raise
    except ValueError:
        raise
    except RuntimeError:
        raise
    else:
        return handle


def plugin_reconfigure(handle, new_config):
    """ Reconfigures the plugin

    Args:
        handle: handle returned by the plugin initialisation call
        new_config: JSON object representing the new configuration category for the category
    Returns:
        new_handle: new handle to be used in the future calls
    """
    _LOGGER.info("Old config for playback plugin {} \n new config {}".format(handle, new_config))
    plugin_shutdown(handle)
    # Done to ensure proper shutdown of plugin
    time.sleep(10)
    new_handle = plugin_init(new_config)

    if new_handle['mode']['value'] == 'async':
        plugin_start(new_handle)
    return new_handle


def plugin_shutdown(handle):
    """ Shutdowns the plugin doing required cleanup, to be called prior to the South plugin service being shut down.

    Args:
        handle: handle returned by the plugin initialisation call
    Returns:
        plugin shutdown
    """
    _LOGGER.info('csv playback Plugin Shutting down')
    global reader
    reader.shutdown_plugin = True
    _LOGGER.debug("Shutdown flag of csv reader set true.")
    if handle['mode']['value'] == 'async':
        global producer, consumer, wait_event, condition, readingsQueue, mode
        # The wait event flag needs to be set to shut down the plugin
        wait_event.set()
        time.sleep(2)  # It is done to allow the consumer thread to figure out that wait_event flag has been set.

        if producer is not None:
            producer._tstate_lock = None
            producer = None
        if consumer is not None:
            consumer._tstate_lock = None
            consumer = None
        if readingsQueue:
            readingsQueue = []
        condition = None

    _LOGGER.info('csv playback Plugin Shut down.')


if not POLL_MODE:
    def plugin_register_ingest(handle, callback, ingest_ref):
        """Required plugin interface component to communicate to South C server async mode

        Args:
            handle: handle returned by the plugin initialisation call
            callback: C opaque object required to passed back to C->ingest method
            ingest_ref: C opaque object required to passed back to C->ingest method
        """
        global c_callback, c_ingest_ref
        c_callback = callback
        c_ingest_ref = ingest_ref

    def plugin_start(handle):
        """ Extracts data from the CSV and returns it in a JSON document as a Python dict.
        Available for async mode only.

        Args:
            handle: handle returned by the plugin initialisation call
        Returns:
            a playback reading in a JSON document, as a Python dict
        """
        global producer, consumer, condition, readingsQueue, wait_event, reader
        readingsQueue = []

        condition = Condition()
        producer = Producer(handle)
        consumer = Consumer(handle)

        # Clearing the wait event , In case of reconfigure , Shutdown is called . During shutdown the wait_event
        # is set. So if plugin starts again we need to clear this event once again.
        wait_event.clear()
        producer.start()
        consumer.start()

if POLL_MODE:
    def plugin_poll(handle):
        """Required plugin interface component to communicate to South C server poll mode
        Args:
            handle: handle returned by the plugin initialisation call
        """
        global reader
        if reader is None:
            raise ValueError
        if not reader.current_csv_file:
            _LOGGER.info("No file found to play inside the given directory.")
            return None
        else:
            if reader.file_iter:
                readings = next(reader.file_iter, None)
            else:
                _LOGGER.info("The csv file finally found. Playing readings from it.")
                reader.read_csv_file()
                readings = next(reader.file_iter, None)

            if not readings:
                _LOGGER.info('End of file reached.')

                if handle['postProcessMethod']['value'] == 'continue_playing':
                    # reload csv file
                    _LOGGER.info('Replaying it')
                elif handle['postProcessMethod']['value'] == 'delete':
                    _LOGGER.info('Deleting the csv file it')
                    os.remove(reader.current_csv_file)
                elif handle['postProcessMethod']['value'] == 'rename':
                    _LOGGER.info('Renaming the csv file with suffix {}'.format(handle['suffixName']['value']))
                    rename_name = reader.current_csv_file + handle['suffixName']['value']
                    os.rename(reader.current_csv_file, rename_name)

                if handle['postProcessMethod']['value'] != 'continue_playing':
                    # Reset the current file.
                    reader.current_csv_file = None
                    reader.file_iter = None

                    # start the finder thread once again.
                    reader.start_finder_thread()

                # load the file once again.
                reader.read_csv_file()
                # fetch readings from it.
                if reader.file_iter:
                    readings = next(reader.file_iter, None)
                else:
                    _LOGGER.info("The next file could not be loaded.")
                    readings = None

        # read and return subsequent asset formatted values from csv file
        return readings


class FileFinder(Thread):
    def __init__(self, parent):
        """
        Start the finder thread that looks out for csv files in directory.
        Args:
            parent: The reader class object.
        """
        super(FileFinder, self).__init__()
        self.parent = parent

    def run(self):
        if self.parent.handle['csvDirName']['value'].startswith(_FLEDGE_DATA_PREFIX):
            if len(self.parent.handle['csvDirName']['value'].split("/")) > 1:
                csv_dir = self.parent.handle['csvDirName']['value'].replace(_FLEDGE_DATA_PREFIX, _FLEDGE_DATA)
            else:
                csv_dir = _FLEDGE_DATA
        else:
            csv_dir = self.parent.handle['csvDirName']['value']

        _LOGGER.info("The directory to be searched is {}".format(csv_dir))
        if not os.path.exists(csv_dir):
            raise FileNotFoundError

        csv_file_name_pattern = self.parent.handle['csvFileName']['value']
        full_file_list = csv_dir + '/' + '*'
        found = False
        while not found and not self.parent.shutdown_plugin:
            time.sleep(2)
            file_list = sorted(glob.glob(full_file_list))
            if not file_list:
                _LOGGER.info("There are no files in this directory currently. Waiting for some time.")
                continue
            filtered_files = [f for f in file_list if os.path.split(f)[1].find(csv_file_name_pattern) != -1
                              and os.path.split(f)[1].endswith(('.csv', 'csv.bz2', 'csv.gz'))]
            if not filtered_files:
                _LOGGER.info("There are no csv files in this directory currently. Waiting for some time.")
            else:
                found = True
                # Taking the file name as the first file found.
                _LOGGER.info("File found will play the file {}".format(filtered_files[0]))
                self.parent.current_csv_file = filtered_files[0]


class CSVReader:
    def __init__(self, handle):
        self.handle = handle

        self.is_burst = self.handle['ingestMode']['value'] == 'burst'
        self.is_historic_ts = self.handle['timestampStyle']['value'] in ['copy csv value', 'move csv value']
        self.is_drop_ts = self.handle['timestampStyle']['value'] == 'move csv value'
        self.is_delta_ts = self.handle['timestampStyle']['value'] == 'use csv sample delta'
        self.asset_name = self.handle['assetName']['value']
        self.ts_col = self.handle['timestampCol']['value']
        self.c = datetime.datetime.now(datetime.timezone.utc).astimezone()
        self.ts_diff = None
        self.df = None
        self.file_iter = None
        self.process_variable_columns = False
        self.process_metadata = False
        self.meta_data_ingested = False
        self.meta_data = {}
        self.current_csv_file = None
        self.shutdown_plugin = False
        self.finder_thread = None
        self.start_finder_thread()
        self.read_csv_file()

    def start_finder_thread(self):
        self.finder_thread = FileFinder(self)
        self.finder_thread.start()

    def stop_finder_thread(self):
        if self.finder_thread:
            self.finder_thread.join()
            self.finder_thread = None

    def get_csv_file_name(self):
        return self.current_csv_file

    def read_csv_file(self):
        """Creates iterators for retrieving chunks of lines from a csv file, and collections
        of asset messages from the chunks of lines.
        Returns: None
        """

        csv_path = self.get_csv_file_name()
        _LOGGER.debug("The file to be played is {}".format(csv_path))
        if not csv_path:
            return None
        if os.path.isfile(csv_path) and os.path.getsize(csv_path) == 0:
            _LOGGER.error(f"CSV file {csv_path} has zero length")
            raise EOFError
        else:
            self.stop_finder_thread()

        # we read a chunk whose size is based on whether we are returning
        # a second's worth of data if we are in "continuous" mode, otherwise a "burst's" worth of data
        chunksize = int(self.handle['chunkSize']['value'])

        _LOGGER.debug("The chunk size is {}".format(chunksize))
        # Check the header processing method

        should_skip_row = False
        if self.handle['headerMethod']['value'] == 'skip_rows' or \
                self.handle['headerMethod']['value'] == 'pass_in_datapoint':
            should_skip_row = True
            rows_to_skip = int(self.handle['noOfRows']['value'])
            _LOGGER.debug("We need to skip {} rows".format(rows_to_skip))

        # check if variable columns are there
        if self.handle['variableCols']['value'] == 'true':
            _LOGGER.debug("We have variable no of columns per row")
            if should_skip_row:
                self.df = pd.read_csv(csv_path, iterator=True, chunksize=chunksize,
                                      skiprows=rows_to_skip, header=None,
                                      engine='python')
            else:
                self.df = pd.read_csv(csv_path, iterator=True, chunksize=chunksize,
                                      header=None,
                                      engine='python')
            self.process_variable_columns = True
        # Check the column processing method

        else:
            if self.handle['columnMethod']['value'] == 'explicit':
                names = self.handle['useColumns']['value']
                _LOGGER.debug("The column names explicitly given are {}".format(names))
                has_type = ':' in names
                names = [] if names == '' else names.split(',')
                if has_type:
                    typeMap = {
                        'str': 'object',
                        'int': 'int64',
                        'float': 'float64',
                        'bool': 'bool_',
                        'timestamp': 'datetime64'
                    }
                    # column list can have a :type sepcifier
                    org_names = names
                    dtype = {}
                    names = []
                    for n in org_names:
                        if n == '':
                            names.append(n)
                        else:
                            nt = n.split(':')
                            if len(nt) == 1:
                                names.append(n)
                            elif len(nt) == 2:
                                if nt[1] not in ['str', 'int', 'float', 'timestamp', 'bool']:
                                    _LOGGER.error("{} must be in [str, int, float, timestamp, bool]".format(nt[1]))
                                    raise TypeError
                                dtype[nt[0]] = typeMap[nt[1]]
                                names.append(nt[0])
                            else:
                                _LOGGER.error("{} must be of the form <name>:<type>".format(nt))
                                raise ValueError("{} must be of the form <name>:<type>".format(nt))
                else:
                    dtype = None

                if should_skip_row:
                    self.df = pd.read_csv(csv_path, iterator=True, chunksize=chunksize,
                                          header=0,
                                          names=names,
                                          dtype=dtype,
                                          usecols=[n for n in names if n != ''],
                                          skiprows=rows_to_skip)
                else:
                    self.df = pd.read_csv(csv_path, iterator=True, chunksize=chunksize,
                                          header=0,
                                          names=names,
                                          dtype=dtype,
                                          usecols=[n for n in names if n != ''])

            elif self.handle['columnMethod']['value'] == 'pick_from_file':
                _LOGGER.debug("We are picking header names from some index in the file.")
                column_row = int(self.handle['rowIndexForColumnNames']['value'])
                if should_skip_row:
                    self.df = pd.read_csv(csv_path, iterator=True, chunksize=chunksize,
                                          header=column_row, skiprows=rows_to_skip)
                else:
                    self.df = pd.read_csv(csv_path, iterator=True, chunksize=chunksize,
                                          header=column_row)

        if self.handle['headerMethod']['value'] == 'pass_in_datapoint':

            self.process_metadata = True
            with open(csv_path) as fd:
                meta_data_array = [next(fd).strip('\n') for i in range(rows_to_skip)]

            meta_data_string = "_".join(meta_data_array)
            self.meta_data = {self.handle['dataPointForCombine']['value']:
                              meta_data_string}
            _LOGGER.debug("The meta data picked from csv file {}".format(self.meta_data))
            self.meta_data_ingested = False

        self.file_iter = self.file_to_readings()

    def file_to_readings(self):
        """ file_of_readings - convert file of chunks of data into readings messages """
        for chunk in self.df:
            for readings in self.chunk_to_readings(chunk):
                yield readings

    def validate_chunk(self, chunk):
        for col_name in chunk.columns:
            is_nan = chunk[col_name].isnull().values.any()  # check if NaN values

            is_blank = chunk[col_name].astype(str).str.isspace().any()  # check if blank values are there

            if is_nan or is_blank:
                # checking either NaN or blank values
                _LOGGER.error("There are NaN / missing values in the CSV file.")
                _LOGGER.info("Going to shutdown csvplayback plugin.")
                plugin_shutdown(self.handle)

    def chunk_to_readings(self, chunk):
        """ chunk_to_readings -- convert multi-row chunk into "asset messages" containing readings
            modify timestamps to emulate different ingest assumptions
            yield results individually or in a batch
        """
        timestamp = []
        if self.handle['variableCols']['value'] == 'false':

            if self.handle['ignoreNaN']['value'] != 'ignore':
                self.validate_chunk(chunk)
        else:
            auto_prefix = self.handle['autoGeneratePrefix']['value']
            main_dict = {}
            try:
                [main_dict.update({auto_prefix + "_" + str(i + 1): val})
                 for i, val in enumerate(chunk.iloc[0, :].values) if not pd.isnull(val)]
                chunk = pd.DataFrame([main_dict])
            except IndexError:
                return None

        if (self.ts_col != '') and (self.ts_col in chunk) and \
                (self.is_historic_ts or self.is_delta_ts):

            # Modifying the time stamps; calculate new values, drop the old
            if self.is_historic_ts:
                # asset timestamps become the data timestamps
                ts_format = self.handle['timestampFormat']['value']
                org_pd_col = chunk[self.ts_col]
                timestamp = list(pd.to_datetime(org_pd_col, format=ts_format).array)

                if self.is_drop_ts:
                    # don't include timestamps from files in actual readings
                    chunk.drop(labels=self.ts_col, axis=1, inplace=True)
            else:  # is_delta_ts
                # Calculate time difference .This will be added to all readings except the first.
                if self.ts_diff is None:
                    org_pd_col = chunk[self.ts_col]
                    ts_format = self.handle['timestampFormat']['value']
                    ts_array = list(pd.to_datetime(org_pd_col, format=ts_format).array)
                    self.ts_diff = ts_array[1] - ts_array[0]

                for _ in range(chunk.shape[0]):
                    timestamp.append(self.c)
                    self.c = self.c + self.ts_diff

        else:
            # 'use current time'
            now_timestamp = datetime.datetime.now(datetime.timezone.utc).astimezone()
            fraction = 1.0 / (max(1.0, len(chunk)))
            uniform_interval = int(fraction * 1000000)
            useconds = 0

        readings = []
        for row_values in chunk.to_dict('records'):
            if self.is_burst:
                # uniform local timestamp for burst mode
                modified_timestamp = str(utils.local_timestamp())
            elif len(timestamp) != 0:
                # continuous - we've created new timestamps for this chunk, use them
                modified_timestamp = str(timestamp.pop(0))
            else:
                # continuous - else, make up a timestamp based on a consistent delta from current time
                modified_timestamp = str(now_timestamp.replace(microsecond=useconds))
                useconds += uniform_interval

            if self.process_metadata:
                row_values.update(self.meta_data)
                reading = {
                    'asset': self.asset_name,
                    'timestamp': modified_timestamp,
                    'readings': row_values
                }
                self.meta_data_ingested = True
            else:
                reading = {
                    'asset': self.asset_name,
                    'timestamp': modified_timestamp,
                    'readings': row_values
                }

            if self.is_burst:
                readings.append(reading)
            else:
                # continuous mode - return readings as you get them
                # xxx - calculate error from desired vs actual return rate
                yield reading

        if readings != []:
            # burst mode - return all at once
            yield readings

        return None


MAX_QUEUE_CHUNK_CAPACITY = 3


class Producer(Thread):
    def __init__(self, handle):
        """
        Initializes the Producer class to read readings from csv file.
        Args:
            handle: The configuration of the plugin
        """
        super(Producer, self).__init__()
        self.handle = handle
        self.max_capacity = MAX_QUEUE_CHUNK_CAPACITY

        self.handle = handle

    def run(self):
        global readingsQueue, reader

        while not reader.shutdown_plugin:

            if not reader.current_csv_file:
                _LOGGER.info("No file found yet. Waiting...")
                time.sleep(5)
                continue
            if not reader.df:
                _LOGGER.info("File has been found. Playing it in async mode.")
                reader.read_csv_file()
            # If plugin shutdown has been called
            global wait_event
            if wait_event.is_set():
                return

            # Do not exceed maximum capacity of Queue
            condition.acquire()
            if len(readingsQueue) >= self.max_capacity:
                condition.wait()

            try:
                chunk = next(reader.df)
                readingsQueue.append(chunk)
            except StopIteration:

                _LOGGER.info('End of file reached.')

                if self.handle['postProcessMethod']['value'] == 'continue_playing':
                    # reload csv file
                    _LOGGER.info('Replaying it')
                elif self.handle['postProcessMethod']['value'] == 'delete':
                    _LOGGER.info('Deleting the csv file it')
                    os.remove(reader.current_csv_file)
                elif self.handle['postProcessMethod']['value'] == 'rename':
                    _LOGGER.info('Renaming the csv file with suffix {}'.format(self.handle['suffixName']['value']))
                    rename_name = reader.current_csv_file + self.handle['suffixName']['value']
                    os.rename(reader.current_csv_file, rename_name)

                if self.handle['postProcessMethod']['value'] != 'continue_playing':
                    # Reset the current file.
                    reader.current_csv_file = None
                    reader.df = None

                    # start the finder thread once again.
                    reader.start_finder_thread()

                    time.sleep(5)

                # load the file once again.
                reader.read_csv_file()
                # fetch readings from it.
                if reader.df:
                    _LOGGER.info("The next file loaded. Playing it...")
                    chunk = next(reader.df)
                    readingsQueue.append(chunk)
                else:
                    _LOGGER.info("The next file could not be loaded. Waiting")
                    time.sleep(5)
                    continue

            # Notifying the consumer that data has been read.
            condition.notify()
            condition.release()


class Consumer(Thread):
    def __init__(self, handle):
        super(Consumer, self).__init__()
        self.handle = handle

    def run(self):
        global readingsQueue, reader

        # Initialize the current time required for time stamp delta if required

        period = float(self.handle['burstInterval']['value']) / 1000.0 if self.handle['ingestMode']['value'] == 'burst' \
            else 1
        count = 0
        n_readings = 0
        org_start_time = datetime.datetime.now()

        while not reader.shutdown_plugin:
            # Check if shutdown is called

            if not reader.current_csv_file:
                time.sleep(5)
                continue
            global wait_event
            if wait_event.is_set():
                return

            # Check if queue is empty
            condition.acquire()
            if len(readingsQueue) == 0:
                condition.wait()
            start_time = datetime.datetime.now()

            chunk = readingsQueue.pop(0)
            if chunk is _sentinel:
                readingsQueue.append(_sentinel)
                break

            for readings in reader.chunk_to_readings(chunk):
                # Ingest into database
                # print("consumer: ingest ", readings)
                count += 1
                n_readings += 1 if (type(readings) == dict) else len(readings)
                async_ingest.ingest_callback(c_callback, c_ingest_ref, readings)

            end_time = datetime.datetime.now()

            if TIME_IT and (n_readings % 10000) == 0:
                duration = (end_time - org_start_time).total_seconds()
                print(f"start: {start_time}")
                print("end: {} readings, {} ({:.3f} total sec)".format(n_readings, end_time, duration))
                print("end: {} per poll, {:.3f} polls/sec, {:.3f} readings/sec".format(duration / count,
                                                                                       count / duration,
                                                                                       n_readings / duration))

            # Notifying the Producer
            condition.notify()
            condition.release()

            # Wait for a fixed interval of time
            timeout = max(period - (end_time - start_time).total_seconds(), 0.0)
            wait_event.wait(timeout=timeout)
