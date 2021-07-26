=========================
foglamp-south-csvplayback
=========================

FLedge South Plugin for CSV playback using either async or poll mode.

Overview
=====================
The csvplayback plugin reads a csv file found in the /usr/local/fledge/data/ directory (or FLEDGE_DATA if that
environment variable is used to override the default). It creates a stream of "readings" which are sent
north towards filters and the database.

Csvplayback can infer column names from the csv file if headers are present. Headers names can also, optionally,
be manually overridden. Columns of the csv file can be omitted so they aren't included in generated readings.
The type of columns may also be specified if there is a preferred override (typically, forcing numbers or
timestamps to strings to prevent conversions).

Csvplayback has options to work in either "continuous" mode where readings are delivered as continuously, or
"burst" mode where multiple readings are delivered in a group at regular intervals. The default is "burst".

Csvplayback has options that allow asset timestamps to be taken from the data file (moved or copied),
reproduced using the sample spacing found in the data file, or derived based on the current (actual) time.

The csvplayback plugin operates either in poll or async mode. Async provides higher potential throughput and
closer to realtime performance, but requires advanced plugin settings to avoid bottlenecks
that can cause high memory usage. Poll mode may have somewhat lower throughput but responds naturally
to overall system performance needs. Async mode simulates realtime playback by inserting delays between
bursts or samples to simulate the original rythm of the data, based on the file timestamps
and plugin configuration settins.

Poll vs async mode is controlled by a global POLL_MODE setting in csvplayback.py and cannot be
modified at runtime.


Plugin Configuration Options
============================

.. code-block:: console

    - 'assetName': The name of the asset readings will be associated with.
    - 'csvFilename': The name of the CSV file with extension. The file must be located in the default FLEDGE_DATA
      data directory.
    - 'useColumns': If unset, headers are inferred from the csv file contents. If set, contains a comma separated list of columns to include. Empty entries are excluded. Columns will be renamed if the name given doesn't match the header name. An optional type may be included, separated by a ':'. Types include str, int, float, bool, timestamp (eg., "channel1:str").
      If set, the value is a comma-separated list of names which overrides any header column names from the csv
      file. If a column name is omitted, the data in that column is omitted from the readings that are generated. 
    - 'ingestMode': Mandatory: Choose one from 'burst' or 'continuous'.
    - 'sampleRate': Mandatory: Ingest rate in samples per second (may be overridden in timestamp values if timestampFromFile or timestampFromDelta are selected).
    - 'burstInterval': Mandatory for 'burst' mode: Interval in ms between two consecutive bursts. Burst size is
      derived from the 'sampleRate' and 'burstInterval' (sampleRate * (burstInterval/1000)). The default is "burst".
    - 'timestampStyle': For "continuous" mode, select one of "current time", "copy csv value", "move csv value", or "use csv sample delta".
    - 'timestampCol': Mandatory if timestampStyle, 'use csv data' / 'use csv sample delta', are used.
      Specify the timestamp column name found in the csv file's header or "useColumns" override values.
    - 'timestampFormat': Mandatory if timestampCol is used.
      Express the value in Python timestamp format.
      The default is '%Y-%m-%d %H:%M:%S.%f'. If timestamp format is not known, provide 'None'. In that case, system will try to guess the timestamp and this will be slower.
    - 'ignoreNaN': Ignore NaN values or report an error if NaN value is found and stop the plugin. Default (Ignore)
    - 'repeatLoop': Allows files to be read in a loop for infinite streams of data.
