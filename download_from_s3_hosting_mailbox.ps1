param ([string]$ingestion_config_filename = "ingestion_mailbox_config.json")
# To read command line parameters the first line of a ps-script must be param (...)!
$rootFolder = $PSScriptRoot
if (-not (Test-Path "$rootFolder\$ingestion_config_filename")) 
{
    throw [System.IO.FileNotFoundException] "$ingestion_config_filename not found in $rootFolder"
}

$today = Get-Date
$ingestion_config = (Get-Content -Raw -Path "$rootFolder\$ingestion_config_filename" | Out-String | ConvertFrom-Json)
$s3Bucket = $ingestion_config.ingest_from
$cumulusDataFolder = $ingestion_config.data_folder
$data_sources = $ingestion_config.data_sources
$tablesJsonFilePath = "$rootFolder\" + $ingestion_config.tables_to_download_config_file
$ingestionDateString = $ingestion_config.ingestion_date
$logfile = $ingestion_config.logs_folder+"\download_from_s3_hosting_mailbox."+$today.tostring("yyyy-MM")+".log"

Function LogWrite
{
   Param ([string]$logstring)
   Write-Output $logstring
   Add-content $logfile -value "$(get-date -f yyyy-MM-dd_hh:mm:ss)`t$logstring"
}

LogWrite("============================================================================")
LogWrite("Starting the execution")

try {
    $date = $today
    if($ingestionDateString)
    {
        $date = [datetime]::ParseExact($ingestionDateString, "yyyy-MM-dd", $null)
    }  
    $year = $date.tostring("yyyy")
    $month = $date.tostring("MM")
    $day = $date.tostring("dd")

    if (-Not(Test-S3Bucket -BucketName $s3Bucket))
    {
        LogWrite("Bucket $s3Bucket was not found")
        LogWrite("Execution terminated.")
        exit
    }

    LogWrite("Downloading data from $s3Bucket bucket...")
        
    $json = Get-Content -Raw -Path $tablesJsonFilePath | Out-String | ConvertFrom-Json
    
    foreach($data_source in $data_sources)
    {
        LogWrite "`nDownloading Data source $data_source`n"
        foreach($table in $json)
        {
            if(-Not $table.is_enabled)
            {
                LogWrite "$($table.source) is disabled"
                continue
            }
            $tablename = $table.source
            $location = "$data_source/$tablename/$year/$month/$day"
            $files = Get-S3Object -BucketName $s3Bucket -Key $location | Select-Object -ExpandProperty key
            if($files.Length -lt 1)
            {
                LogWrite "There is nothing to download from $location"
                continue
            }
            foreach($file in $files)
            {
                Copy-S3Object -BucketName $s3Bucket -Key $file -LocalFile "$cumulusDataFolder/$file" #-ProfileName prodaccess #Important! Uncomment Parameter only for running on local machine. Will not work on prod with it
            }
        }
    }
    LogWrite("The download went successfully.")
}
catch {
    LogWrite($_.Exception.Message)
}

LogWrite("Execution completed.")