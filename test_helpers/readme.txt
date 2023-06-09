to set stuff up:
configure test_config.ini
OPYRND_CONFIG=test.config.ini python3 make_example_class.py
OPYRND_CONFIG=test.config.ini python3 make_example_jr.py
{this prints a job id number}
OPYRND_CONFIG=test.config.ini python3 ../cromwell2operend.py meta_big_job.json manifest_big_job.json {job id number goes here} --mock-file=small_file

The --mock-file argument is telling the script not to look in the actual filesystem, and instead to just open the one small_file over and over for the mock contents of every file. If you have metadata that refers to a filesystem you actually have mounted, leave out the --mock-file argument to use the real files.

If you omit the job id number, the script will just work on entities and not interact with any job runs.

Additional useful arguments:
--list: this makes the script just dump information about the inputs and outputs as seen in the metadata json.
--very-dry-run: this makes the script not try to connect to Operend at all, and only do the steps it can do without connecting.
--dry-run: this makes the strip connect to Operend for read-only operations, but not do the steps that write back.



