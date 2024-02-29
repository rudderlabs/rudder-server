from aiokafka import AIOKafkaConsumer
import asyncio
import os
import uuid
import sys
import json

async def consume():

    kafkaBroker = os.environ.get('KAFKA_BROKER')
    kafkaTopic = os.environ.get('KAFKA_TOPIC')

    msg_count = int(os.environ.get('EXPECTED_MESSAGE_COUNT', 0), 10)

    print(f"Kafka Broker: {kafkaBroker}", file=sys.stderr)
    print(f"Kafka Topic: {kafkaTopic}", file=sys.stderr)
    print(f"Expected Message Count: {msg_count}", file=sys.stderr)

    consumer = AIOKafkaConsumer(
        kafkaTopic,
        bootstrap_servers=kafkaBroker,
        group_id="aiokafka-test-"+uuid.uuid4().__str__(),
        auto_offset_reset="earliest",
    )
    # print(await consumer.topics(), file=sys.stderr)

    output = list()
    await consumer.start()
    for _ in range(msg_count):
        msg = await consumer.getone()
        output.append({
            "key": msg.key.decode('utf-8'),
            "value": msg.value.decode('utf-8'),
        })

    await consumer.stop()
    print(json.dumps(output), file=sys.stdout)

asyncio.run(consume())
