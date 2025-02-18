# buzzline-06-alvaroq

This module is our opportunity to build a unique streaming data pipeline tailored to our interests or domain. We’ll integrate things we've learned so far—producers, consumers, data formats, analytics, visualizations, and/or databases—into a cohesive project of our choice. Being able to apply our skills to design and implement streaming data solutions is key. 

## VS Code Extensions

- Black Formatter by Microsoft
- Markdown All in One by Yu Zhang
- PowerShell by Microsoft (on Windows Machines)
- Pylance by Microsoft
- Python by Microsoft
- Python Debugger by Microsoft
- Ruff by Astral Software (Linter)
- SQLite Viewer by Florian Klampfer
- MongoDB for VSCode
- WSL by Microsoft (on Windows Machines)

## Task 1. Use Tools from Module 1 and 2

Before starting, ensure completion of the setup tasks in <https://github.com/denisecase/buzzline-01-case> and <https://github.com/denisecase/buzzline-02-case> first. 

Versions matter. Python 3.11 is required. See instructions for the required Java JDK and more. 

## Task 2. Copy This Example Project and Rename

Once the tools are installed, copy/fork this project into your GitHub account
and create your own version of this project to run and experiment with. 
Follow the instructions in [FORK-THIS-REPO.md](https://github.com/denisecase/buzzline-01-case/docs/FORK-THIS-REPO.md).
    

## Task 3. Manage Local Project Virtual Environment

Follow the instructions in [MANAGE-VENV.md](https://github.com/denisecase/buzzline-01-case/docs/MANAGE-VENV.md) to:
1. Create your .venv
```zsh
python3 -m venv .venv
```
1. Activate .venv
```zsh
source .venv/bin/activate
```
1. Install the required dependencies using requirements.txt.
```zsh
python3 -m pip install --upgrade pip setuptools wheel
python3 -m pip install --upgrade -r requirements.txt
```

## Task 4. Start Zookeeper and Kafka (Takes 2 Terminals)

If Zookeeper and Kafka are not already running, you'll need to restart them.
See instructions at [SETUP-KAFKA.md] to:

1. Start Zookeeper Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-7-start-zookeeper-service-terminal-1))
```zsh
cd ~/kafka
chmod +x zookeeper-server-start.sh
bin/zookeeper-server-start.sh config/zookeeper.properties
```
1. Start Kafka Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-8-start-kafka-terminal-2))
```zsh
cd ~/kafka
chmod +x kafka-server-start.sh
bin/kafka-server-start.sh config/server.properties
```
---

## Task 5. Start a New Streaming Application

This will take two more terminals:

1. One to run the producer which writes messages. 
2. Another to run the consumer which reads messages, processes them, and writes them to a data store. 

### Producer (Terminal 3) 

This script acts as a Kafka producer, fetching sports betting odds data from the Odds API and publishing it to a Kafka topic. It establishes a connection to a Kafka broker, retrieves NBA game data from the Odds API at a user-specified interval, and for each game, constructs a JSON message containing the sport, region, game ID, and betting sites. These messages are then sent to the designated Kafka topic and also written to a local JSON file for logging purposes. Robust error handling is implemented to manage potential issues during API requests and Kafka publishing. The script runs indefinitely, continuously fetching and publishing new data.

Use the commands below to activate .venv, and start the producer. 


Mac/Linux:
```zsh
source .venv/bin/activate
export ODDS_API_KEY="161f62644c47daaafe456111480c83c8" 
python3 -m producers.producer_alvaro
```


### Consumer (Terminal 4) - 

This script functions as a real-time data processing and visualization pipeline, consuming messages from a Kafka topic ("sports_odds"), extracting game IDs, and updating a count for each ID. It concurrently stores these messages in an SQLite database and dynamically updates two Matplotlib charts—a bar chart and a pie chart—to visually represent the game ID counts. Error handling ensures robustness, and the interactive plotting feature provides a live, continuously updated display of the incoming data. The script relies on environment variables for configuration and gracefully handles cases where data is missing or incomplete.

Use the commands below to activate .venv, and start the consumer. 

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m consumers.db_sqlite_alvaro
```


## License
This project is licensed under the MIT License as an example project. 
You are encouraged to fork, copy, explore, and modify the code as you like. 
See the [LICENSE](LICENSE.txt) file for more.