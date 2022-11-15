#! /usr/bin/env bash

echo "Starting docker..."
docker-compose up -d --build
docker exec -it jupyterlab /opt/conda/bin/jupyter server list  


function clean_up {
    echo "\n\nShutting down...\n\n"
    
    docker-compose down -v
}

trap clean_up EXIT

echo '''


==============================================================================================================
MongoDB Spark Connector POV

Jupyterlab
'''

docker exec -it jupyterlab /opt/conda/bin/jupyter server list  

echo '''
Spark Master - http://127.0.0.1:8080
Spark Worker 1 - http://127.0.0.1:8081
Spark Worker 2 - http://127.0.0.1:8082
JupyterLab - http://127.0.0.1:8888/?token=[your_generated_token]

==============================================================================================================

Use <ctrl>-c to quit'''

read -r -d '' _ </dev/tty
echo '\n\nTearing down the Docker environment, please wait.\n\n'

dockder-compose down -v

# note: we use a -v to remove the volumes, else you will end up with old data
