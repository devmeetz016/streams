
import {  createClient, RedisClientType } from 'redis';
import { STREAM_KEY_NAME } from './redis';
import { randomInRange, sleep } from '../utils';

const CONSUMER_GROUP_NAME = "temphumidity_consumers";
const BLOCK_TIME = 5000;
const MIN_WORK_DURATION = 1000;
const MAX_WORK_DURATION = 5000;

export async function   streamConsumerInGroup (count: number  ){
  const client: RedisClientType = createClient({
		url: `rediss://default:okdemsg1b367@4d70zq6gm.dragonflydb.cloud:6385`,
	});

	await client.connect();
  await ensureStreamAndGroup(client, STREAM_KEY_NAME, CONSUMER_GROUP_NAME);

  const consumerName = `consumer${count}`;
  console.log(`Starting consumer ${consumerName}...`);
  let consumerGroupStatus = 'created';

  try {
    await client.xGroupCreate(STREAM_KEY_NAME, CONSUMER_GROUP_NAME, 0);
  } catch (err) {
    consumerGroupStatus = 'exists, not created';  
  }

  console.log(`Consumer group ${CONSUMER_GROUP_NAME} ${consumerGroupStatus}.`);
  
  while (true) {
    console.log('Reading stream...');
    const response = await client.xReadGroup(
      CONSUMER_GROUP_NAME, // Consumer Group name
      consumerName,        // Consumer name
      [
        { key: STREAM_KEY_NAME, id: '>' } // Stream and the starting ID
      ], 
      { 
        COUNT: 1,     // Number of messages to fetch
        BLOCK: BLOCK_TIME // Time to block if no message
      }
    );
    
    
    if (response) {
      // Unpack some things from the response...
      const entry = response[0].messages[0];
      
      console.log(`Received entry ${entry.id}:`);
      const safeObject = Object.fromEntries(Object.entries(entry.message));
			console.log(safeObject);

      // Simulate some work
      await sleep(randomInRange(MIN_WORK_DURATION, MAX_WORK_DURATION, true));

      // Positively acknowledge this message has been processed.
      await client.xAck(STREAM_KEY_NAME, CONSUMER_GROUP_NAME, entry.id);
      console.log(`Acknowledged processing of entry ${entry.id}.`);
    } else {
      console.log('No new entries yet.');
    }
  }
};
async function ensureStreamAndGroup(
  client: RedisClientType,
  stream: string,
  group: string
) {
  try {
    // Ensure the stream exists by adding a dummy entry if it's empty
    const streamExists = await client.exists(stream);
    if (!streamExists) {
      await client.xAdd(stream, '*', { init: '1' });
      console.log(`Stream ${stream} created with dummy data.`);
    }

    // Try to create group
    await client.xGroupCreate(stream, group, '0', {
      MKSTREAM: true, // create stream if not exists (for safety)
    });

    console.log(`Consumer group ${group} created.`);
  } catch (err: any) {
    if (err?.message?.includes('BUSYGROUP')) {
      console.log(`Consumer group ${group} exists, not created.`);
    } else {
      console.error(`Error creating consumer group:`, err);
    }
  }
}
