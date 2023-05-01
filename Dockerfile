FROM operend/opyrnd:1.2


RUN mkdir /operend_scripts
COPY fastq_copy.py /operend_scripts/
COPY get_workfile.py /operend_scripts/
WORKDIR /opyrnd_scripts
