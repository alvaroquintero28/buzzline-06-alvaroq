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

This Python script functions as a Kafka producer, continuously fetching real-time sports odds data from The Odds API and publishing it to a Kafka topic.  It uses environment variables for secure configuration, incorporates robust error handling and logging for both API interactions and Kafka communication, and transforms the raw API data into structured JSON messages before sending them.  The script runs indefinitely, sending data at a user-specified interval, ensuring a continuous stream of sports odds information is made available for consumers.

Use the commands below to activate .venv, and start the producer. 

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m producers.producer_alvaro
```


### Consumer (Terminal 4) - 

This Python script acts as a Kafka consumer, designed to receive messages from a specified Kafka topic and store them in a local SQLite database.  It begins by establishing a connection to the Kafka broker and the designated topic, using environment variables to configure the broker address and topic name.  Upon successful connection, it reads messages from the Kafka topic one by one.  If a database table named sports_odds does not already exist, it will create one to store the incoming messages. Each received message is then converted from JSON format and inserted into the sports_odds table within the SQLite database.  The script incorporates error handling to manage potential issues during database operations and Kafka consumption, and it ensures the Kafka consumer is closed gracefully using a finally block.  The main function serves as a simple entry point for the database operations, and you would remove this when integrating the consumer function into a larger application.

Use the commands below to activate .venv, and start the consumer. 

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m consumers.db_sqlite_alvaro
```

---

## Review the Project Code

Review the requirements.txt file. 

Review the .env file with the environment variables.

Review the .gitignore file.

Review the code for the producer and the two consumers.

Compare the consumer that reads from a live data file and the consumer that reads from a Kafka topic.

What files are in the utils folder? 

What files are in the producers folder?

What files are in the consumers folder?

---

## Explorations

- Did you run the kafka consumer or the live file consumer? Why?
- Can you use the examples to add a database to your own streaming applications? 
- What parts are most interesting to you?
- What parts are most challenging? 

---

## Later Work Sessions
When resuming work on this project:
1. Open the folder in VS Code. 
2. Open a terminal and start the Zookeeper service. If Windows, remember to start wsl. 
3. Open a terminal and start the Kafka service. If Windows, remember to start wsl. 
4. Open a terminal to start the producer. Remember to activate your local project virtual environment (.env).
5. Open a terminal to start the consumer. Remember to activate your local project virtual environment (.env).

## Save Space
To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later. 
Managing Python virtual environments is a valuable skill. 

## License
This project is licensed under the MIT License as an example project. 
You are encouraged to fork, copy, explore, and modify the code as you like. 
See the [LICENSE](LICENSE.txt) file for more.