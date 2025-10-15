## Dry run

`python submit_jobs.py config.json `

After generating all required files, the script will print the submission commands (e.g., condor_submit submit_QCD_HT-1000to1200.jdl). You can change to the job directory and run these manually or run automatic submission (below)

## Automatic submission

`python submit_jobs.py config.json 1`
