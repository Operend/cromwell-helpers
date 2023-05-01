export PATH=/homw/user/development/opyrnd/venv/bin:$PATH
export PYTHONPATH=/home/user/development/opyrnd:$PYTHONPATH

python fastq_copy.py "http://operend.bu.edu/api" "usr:tok_name:somelongstring-" "false" "_class=FASTQSet&_full_text=SOME_SEARCH_TEXT" "temp_dir"


