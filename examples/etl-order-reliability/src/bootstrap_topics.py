from __future__ import annotations

import asyncio

from settings import get_settings


async def main() -> None:
    try:
        from aiokafka.admin import AIOKafkaAdminClient, NewTopic
        from aiokafka.errors import TopicAlreadyExistsError
    except ImportError:
        raise ImportError(
            "bootstrap_topics requires aiokafka. Install the example dependencies first."
        ) from None

    settings = get_settings()
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka.bootstrap_servers)
    await admin.start()
    try:
        topics = [
            NewTopic(
                name=settings.kafka_raw_topic,
                num_partitions=6,
                replication_factor=3,
            ),
            NewTopic(
                name=settings.kafka_clean_topic,
                num_partitions=6,
                replication_factor=3,
            ),
        ]
        try:
            await admin.create_topics(topics)
            print(f"Created topics: {settings.kafka_raw_topic!r}, {settings.kafka_clean_topic!r}")
        except TopicAlreadyExistsError:
            print("Kafka topics already exist; nothing to do.")
    finally:
        await admin.close()


if __name__ == "__main__":
    asyncio.run(main())
