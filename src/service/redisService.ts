import { Redis } from '@upstash/redis/cloudflare';

export async function readFromStream() {
	const redis = new Redis({
		url: `https://learning-caribou-32038.upstash.io`,
		token: `AX0mAAIjcDE1NmViMzk1M2Y3YTg0YTE0OTc2MWNjODA2MmRlNjIyNXAxMA`,
	});
	try {
		const messages = await redis!.xread('k-stream', '0');
		console.log('Sample messages:', messages);
		return messages;
	} catch (error) {
		console.log('Error reading from stream', error);
	}
}
