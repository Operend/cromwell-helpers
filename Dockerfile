FROM operend/opyrnd:1.4


RUN mkdir /operend_scripts
COPY fastq_copy.py /operend_scripts/
COPY get_workfile.py /operend_scripts/
COPY cromwell2operend.py /operend_scripts/
# might as well toss the testing stuff in there for now
COPY test_helpers/ /operend_scripts/test_helpers/
WORKDIR /opyrnd_scripts
