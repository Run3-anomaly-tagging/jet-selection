**Usage**

For a dry run:

`python submit_jobs.py config.json `

After generating all required files, the script will print the submission commands (e.g., condor_submit submit_QCD_HT-1000to1200.jdl). You can copy and run these manually.

For automatic submission

`python submit_jobs.py config.json 1`
