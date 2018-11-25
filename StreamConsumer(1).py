from kafka import KafkaConsumer

import json



def main():
    '''
    Consumer consumes tweets from producer
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer('twitter')
    for msg in consumer:
        output = []
        output.append(json.loads(msg.value))
        print(output)
        print('\n')




if __name__ == "__main__":
    main()
