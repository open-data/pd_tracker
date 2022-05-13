#!/bin/sh
#
# Re-read the recombinant schemas from the Open Canada portal and import them into the PD Tracker database.
# No arguments are required.
#

# activate the PD Tracker Python virtual environment before running.
source ./venv/bin/activate

declare -a pdtypes=("adminaircraft" "ati" "briefingt" "consultations" "contracts" "contractsa" \
                    "dac" "experiment" "grants" "hospitalityq" "inventory" "nap" "qpnotes" \
                    "reclassification" "service" "travela" "travelq" "wrongdoing")

# Call the Django custom command to re-read the schemas from the Open Canada portal.
for pdtype in "${pdtypes[@]}"
do
  python ./manage.py import_ckan_schema  -t $pdtype
  echo "Imported $pdtype"
done
date

