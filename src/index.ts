import { readFromStream } from "./service/redisService";
import {Env} from "./types/env";
import {StreamConsumer} from "./service/redisQueueService";


const redisTest=async ()=>{
	console.log("Test function for redis....")
	await readFromStream();
}

// redisTest();
export default {
	async fetch(request, env:Env, ctx): Promise<Response> {
		let objectId = env.STREAMCONSUMER.idFromName('foo');
		const objectStub = env.STREAMCONSUMER.get(objectId);

        // Start continuous Redis polling
		return objectStub.fetch(request);
	},
} satisfies ExportedHandler<Env>;


export {StreamConsumer}
