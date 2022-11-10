function Wait-KeyPress
{
    param
    (
        [ConsoleKey]
        $Key = [ConsoleKey]::Escape
    )
    do
    {
        $keyInfo = [Console]::ReadKey($false)
    } until ($keyInfo.Key -eq $key)
}

Write-Host  "Starting docker..."

docker-compose up -d --build

==============================================================================================================

MongoDB Spark Connector POV

Jupyterlab"

docker exec -it jupyterlab  /opt/conda/bin/jupyter server list

Write-Host  -ForegroundColor Yellow "NOTE: When running on Windows change 0.0.0.0 to 127.0.0.1 in the URL above"

Write-Host "

Spark Master - http://127.0.0.1:8080
Spark Worker 1 - http://127.0.0.1:8081
Spark Worker 2 - http://127.0.0.1:8082
JupyterLab - http://127.0.0.1:8888/?token=[your_generated_token]

==============================================================================================================

Press <Escape> to quit"

Wait-KeyPress
 
docker-compose down  -v

# note: we use a -v to remove the volumes, else you'll end up with old data
