import {Env} from "./types/env";
import {StreamConsumer} from "./service/consumerService";
// redisTest();
export default {
	async fetch(request, env:Env, ctx): Promise<Response> {
		let objectId = env.STREAMCONSUMER.idFromName('foo');
		const objectStub = env.STREAMCONSUMER.get(objectId);
		return objectStub.fetch(request);
	},
} satisfies ExportedHandler<Env>;


export {StreamConsumer}
