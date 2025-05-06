import { Env } from '../types/env';
import { createClient, RedisClientType } from 'redis';
import { connect, StringCodec, credsAuthenticator, AckPolicy, JetStreamClient } from 'nats.ws';
import { streamConsumerInGroup } from './consumerGroup';
import { randomInRange, sleep } from '../utils';
export class StreamConsumer implements DurableObject {
	redis: RedisClientType | null = null;
	isRunning: boolean;
	env: Env;
	lastId: string;
	count: number = 0;
	nc: any = null;
	LAST_ID_KEY = 'consumer:lastid';
	consumer: string = 'k-consumer';
	initPromise: Promise<void>;
	creds = `-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJOQlAyNzJEQUg3QVBKNU5SRVNZNU1SQVZZWUFKTFlFVlROR0dQWEc3NDVOVUpaNVJOUEVBIiwiaWF0IjoxNzQ2MTg4ODU4LCJpc3MiOiJBQUJVUFBNRk5JVFZWWVFXQVJVWUhWTTVHTlRYRTNXRVNQVlZOT1VJN01DV0ZZSUhORjJLSDVENSIsIm5hbWUiOiJrLWNvbnN1bWVyIiwic3ViIjoiVUE3QzRXUTVESUk2M0JENUpZUkJOVFpUVTRCQ0JLM0JNV1gzQUxXR1ZBREhUN0U0WEhLNE5SSEgiLCJuYXRzIjp7InB1YiI6eyJhbGxvdyI6WyIqIiwiX0lOQk9YLlx1MDAzZSIsIiRKUy5BUEkuXHUwMDNlIl19LCJzdWIiOnsiYWxsb3ciOlsiKiIsIl9JTkJPWC5cdTAwM2UiXX0sInN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsImlzc3Vlcl9hY2NvdW50IjoiQUFLU1NFU0s1QVdGVFlEWlFCU0FZWllXSzRTRkJMUktIQ1BDNENHR1pURFNXVkROTUo3UE00UkIiLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.-TRQE1Hccpjtl23WnijJ-ytTFqgyWLnBTe-5xLVCgMKhA_XjNnHfMxClBiv5adtZNmoHcd50dWAREvNQ7StUBQ
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
SUALLF4C7KFA4F5PD7ZXWQTVFLJU5FZJFGFQZNDUXIPLU3XURHGF7ROWBM
------END USER NKEY SEED------

*************************************************************
`; //store this in kv in future currently hardcoded value

	constructor(state: DurableObjectState, env: Env) {
		this.env = env;
		this.isRunning = false;
		this.lastId = '0';

		// Initialize NATS connection only once
		this.initPromise = this.initJetStream(); // <-- added
	}

	async initUpstashRedis() {
		if (!this.redis) {
			this.redis = createClient({
				url: this.env.UPSTASH_REDIS_URL,
			});
			await this.redis.connect();
		}
	}

	async print(latency: number, id: string, data: any) {
		// console.log('Latency (ms):', latency);
		// console.log('ID:', id);
		console.log('Data:', data.message);
	}

	async initJetStream() {
		try {
			if (!this.nc) {
				this.nc = await connect({
					servers: 'wss://connect.ngs.global:443',
					timeout: 10000,
					authenticator: credsAuthenticator(new TextEncoder().encode(this.creds)),
				});
				console.log('NATS client initialized');
			}
		} catch (error) {
			console.log('Error connecting to NATS:', error);
		}
	}

	async readFromDragonfly() {
		this.count++;
		await streamConsumerInGroup(this.count);
		// await streamConsumer();
	}

	async readFromRedisStream() {
		try {
			if (!this.redis) {
				await this.initUpstashRedis();
			}
			while(true){
				const messages: any = await this.redis!.xRead(
					[
						{
							key: this.env.UPSTASH_REDIS_STREAM_NAME,
							id: this.lastId,
						},
					],
					{
						COUNT: 100,
						BLOCK: 0,
					}
				);
				if (messages) {
					// Unpack response...
					const entry = messages[0].messages[0];
	
					console.log(`Received entry ${entry.id}:`);
					const safeObject = Object.fromEntries(Object.entries(entry.message));
					console.log(safeObject);
	
					// Simulate some work
					await sleep(randomInRange(1000, 5000, true));
					console.log(`Finished working with entry ${entry.id}`);
	
					// Update the last ID we have seen.
					this.lastId = entry.id;
					await this.redis!.set(this.LAST_ID_KEY, this.lastId);
					console.log(`Stored last ID ${this.lastId}`);
				} else {
					console.log(`No new entries since entry ${this.lastId}.`);
				}
			}
		} catch (error) {
			console.log('Error reading from stream', error);
			return null;
		}
	}

	async readFromJetStream() {
		try {
			await this.initPromise; // <-- wait for init only once
			const jsm = await this.nc.jetstreamManager();
			const js: JetStreamClient = await this.nc.jetstream();
			const consumer = await js.consumers.get('k-stream', this.consumer);
			const sc = StringCodec();
			const messages = await consumer.consume({
				max_messages: 10, // required for consumer.fetch to do batch processing
				expires: 10000,
				idle_heartbeat: 1000,
			});

			for await (const msg of messages) {
				console.log('Received message from nats jetStream:', sc.decode(msg.data));
			}
		} catch (error) {
			console.error('error:', error);
		}
	}

	async continuousPolling() {
		if (this.isRunning) return;

		this.isRunning = true;
		let pollInterval = 2000; // Base interval
		while (this.isRunning) {
			// const hasMessages = (await this.readFromRedisStream()) || (await this.readFromJetStream());
			const hasMessages = await this.readFromRedisStream();

			// Adjust polling interval based on message activity
			pollInterval = hasMessages ? 100 : Math.min(pollInterval * 1.5, 5000);

			await this.waitFor(pollInterval);
		}
	}

	waitFor(ms: number) {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}

	async stopContinuousPolling() {
		this.isRunning = false;
		console.log('Stopping polling...');

		// Clean up resources
		if (this.nc) {
			await this.nc.close();
			this.nc = null;
		}
	}

	async fetch(request: Request) {
		// await this.continuousPolling();
		// await this.readFromJetStream();
		await this.readFromDragonfly();
		// await this.readFromRedisStream();
		return new Response('This Durable Object Consumes from streams.');
	}
}
