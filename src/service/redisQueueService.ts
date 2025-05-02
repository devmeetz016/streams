import { Redis } from '@upstash/redis/cloudflare';
import { Env } from '../types/env';

export class StreamConsumer implements DurableObject {
	redis: Redis;
    isRunning: boolean;
	env: Env;
	lastId:string;

    constructor(state: DurableObjectState, env: Env) {
		this.env = env;	
        this.redis = new Redis({
            url: this.env.UPSTASH_REDIS_URL,
            token: this.env.UPSTASH_REDIS_TOKEN,
        });
        this.isRunning = false;
		this.lastId = '0';
    }
    async readFromStream() {
        try {
            const messages:any = await this.redis.xread(this.env.UPSTASH_REDIS_STREAM_NAME, this.lastId);
			if (messages && messages.length > 0) {
				// console.log('New messages:', messages);
				const [streamName, entries] = messages[0];
				// Update lastId to the latest one we received
				for (const [id, keyValueArray] of entries) {
					const data: Record<string, string> = {};
					for (let i = 0; i < keyValueArray.length; i += 2) {
					  const key = keyValueArray[i];
					  const value = keyValueArray[i + 1];
					  data[key] = value;
					}
				
					console.log('ID:', id);
					console.log('Data:', data);
					this.lastId = id;  //last id
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

    async continuousPolling() {
        if (this.isRunning) return;

        this.isRunning = true;
        console.log("Starting continuous Redis polling...");
        while (this.isRunning) {
            console.log("Reading from Redis stream...");
            await this.readFromStream();
            await this.waitFor(2000);  // timeout 
        }
    }

    // artificial delay
    waitFor(ms: number) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

	//incase we want to setop
    async stopContinuousPolling() {
        this.isRunning = false;
        console.log("Stopped continuous Redis polling.");
    }

    
    async fetch(request: Request) {
		await this.continuousPolling()
        return new Response('This Durable Object handles Redis operations.');
    }
}
