import { Redis } from '@upstash/redis/cloudflare';
import { Env } from '../types/env';
import { connect, StringCodec, credsAuthenticator, AckPolicy, JetStreamClient } from 'nats.ws';

export class StreamConsumer implements DurableObject {
	private natsConnection: any;
	private jetstream: any | null = null;
	redis: Redis;
	isRunning: boolean;
	env: Env;
	lastId: string;
	nc: any = null;
	consumer: string = 'k-consumer';
	initPromise: Promise<void>; // <-- added
	creds = `-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJRTFNDNFhUSE1UNjVGREw0QVRGNVpMTllKQkM2N1EyVlVYNlpDVk0zSks0Q0tDWUJaVUVRIiwiaWF0IjoxNzQ2MTg3Nzc4LCJpc3MiOiJBQUJVUFBNRk5JVFZWWVFXQVJVWUhWTTVHTlRYRTNXRVNQVlZOT1VJN01DV0ZZSUhORjJLSDVENSIsIm5hbWUiOiJrLXN0cmVhbS11c2VyIiwic3ViIjoiVUNHNlRETVQ2M1NBUkxBN1BSNVQzVE1WRVlKN0NSWkI2WjNEN1pSWkdJUzNOQU1MSk9SSVdNVkQiLCJuYXRzIjp7InB1YiI6eyJhbGxvdyI6WyIqIiwiX0lOQk9YLlx1MDAzZSIsIiRKUy5BUEkuXHUwMDNlIl19LCJzdWIiOnsiYWxsb3ciOlsiKiIsIl9JTkJPWC5cdTAwM2UiXX0sInN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsImlzc3Vlcl9hY2NvdW50IjoiQUFLU1NFU0s1QVdGVFlEWlFCU0FZWllXSzRTRkJMUktIQ1BDNENHR1pURFNXVkROTUo3UE00UkIiLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.95WMdkvqnNlx7XgSW26mc8ayRzUEUUM0z-6p-UulGSPrXI-taekWUyDzvqCWizTBqlLtn4lmx_LtwwTyd2KWDw
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
SUAK6CT7SARDHB54G6MXOEZBR4KIG72WPNPM5KPHA6UWKBKP5KG6WECMII
------END USER NKEY SEED------

*************************************************************
`;

	constructor(state: DurableObjectState, env: Env) {
		this.env = env;
		this.redis = new Redis({
			url: this.env.UPSTASH_REDIS_URL,
			token: this.env.UPSTASH_REDIS_TOKEN,
		});
		this.isRunning = false;
		this.lastId = '0';

		// Initialize NATS connection only once
		this.initPromise = this.initJetStream(); // <-- added
	}

	async print(latency: number, id: string, data: any) {
		console.log('Latency (ms):', latency);
		console.log('ID:', id);
		console.log('Data:', data);
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

	async readFromRedisStream() {
		try {
			const messages: any = await this.redis.xread(this.env.UPSTASH_REDIS_STREAM_NAME, this.lastId);
			if (messages && messages.length > 0) {
				const [streamName, entries] = messages[0];
				for (const [id, keyValueArray] of entries) {
					const data: Record<string, string> = {};
					for (let i = 0; i < keyValueArray.length; i += 2) {
						const key = keyValueArray[i];
						const value = keyValueArray[i + 1];
						data[key] = value;
					}
					const [timestampStr] = id.split('-');
					const timestamp = Number(timestampStr);
					const now = Date.now();
					const latency = now - timestamp;
					this.print(latency, id, data);
					this.lastId = id;
				}
			} else {
				console.log('No new messages.');
			}
			return messages;
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
			const messages = await consumer.fetch({
				max_messages: 10,
				expires: 10000,
				idle_heartbeat: 1000,
			});

			for await (const msg of messages) {
				console.log('Received message:', sc.decode(msg.data));
			}
		} catch (error) {
			console.error('error:', error);
		}
	}

	async continuousPolling() {
		if (this.isRunning) return;

		this.isRunning = true;
		while (this.isRunning) {
			console.log("reading from jetstream.....\n\n\n\n\n\n\n");
			await this.readFromJetStream();
			await this.waitFor(2000);
		}
	}

	waitFor(ms: number) {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}

	async stopContinuousPolling() {
		this.isRunning = false;
		console.log('stop polling....');
	}

	async fetch(request: Request) {
		await this.continuousPolling();
		return new Response('This Durable Object handles Redis operations.');
	}
}
