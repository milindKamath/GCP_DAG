# Continuation of the GCP PubSub repo

>> First Module

The GCP pub sub contains the docker file.

When the docker container is executed, the request is triggered.
Then a pubsub topic is created along with the subscriber.
The message from the request is published to the topic.

When a message is published, a cloud function is triggered.
This reads the message from the pubsub and stores into the cloud storage.

>> The second module starts from here

When the message gets stored into the cloud storage, a cloud function is triggered.
This cloud function triggers a DAG.

The dag contains the code to extract data from the cloud storage.
It then transforms the data.
The transformation includes segregating currency price stronger/ weaker than USD.
This data is then loaded into two different tables in the dataset.



