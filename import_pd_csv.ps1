#
# Running the script will perform the PD CSV warehouse comparison script for each of the PD types.
# The script assumes that the PD CSV files are in subdirectories based on the dates being compared.
# For example, if comparing the files for May 1st and April 30th 2022, the script will look for the CSV files
# in the subdirectories:
#   ./data/20220430/
#   ./data/20220501/
# To run the script for this date, pass in the second date as a parameter. For example:
#   python manage.py import_pd_csv 2020-05-01

# activate the PD Tracker Python virtual environment before running.
.\venv\Scripts\activate

$target_date = [DateTime]$args[0]
$source_date = $target_date.AddDays(-1)

$t1 = $target_date.ToString("yyyy-MM-dd")
$s1 = $source_date.ToString("yyyy-MM-dd")

echo "Target (To) Date:   $t1"
echo "Source (From) Date: $s1"

$pdtypes = @("adminaircraft","ati","ati-nil","briefingt","consultations","contracts","contracts-nil","contractsa",`
 "dac","experiment","grants","grants-nil","hospitalityq","hospitalityq-nil","inventory","nap","qpnotes",`
 "reclassification", "reclassification-nil","service","service-std","travela","travelq","travelq-nil","wrongdoing")

$folder1 = $source_date.ToString("yyyyMMdd")
$folder2 = $target_date.ToString("yyyyMMdd")

date
foreach ($pdtype in $pdtypes)
{
  python .\manage.py compare_csv_files --table $pdtype --first_file data\$folder1\$pdtype.csv --second_file  data\$folder2\$pdtype.csv --source_date $s1 --log_date $t1
  Start-Sleep -s 3
}
date