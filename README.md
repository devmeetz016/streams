# K-Client-Streams

A Cloudflare Worker application that demonstrates consuming data from multiple streaming sources using Durable Objects.

## Overview

This project showcases how to consume data from different streaming platforms:
- Upstash Redis Streams
- DragonFly Redis Streams (with consumer groups)
- NATS JetStream

The application uses Cloudflare Workers with Durable Objects to maintain state between requests.

## Current Implementation
- Upstash Redis client uses polling to retrieve data
- DragonFly Redis uses consumer groups or a single consumer for distributed processing
- NATS JetStream consumes data directly from the producer

## Setup

1. Clone the repository
2. Install dependencies:
   ```
   npm install
   ```
3. Deploy to Cloudflare:
   ```
   npm run deploy
   ```

## Development

Run the local development server:
```
npm run dev or npx wrangler dev
```

## Generating Test Data

To produce data for the streams:

1. Navigate to the `k-populate` directory in the same parent folder
2. Start the producer application:
   ```
   npx wrangler dev
   ```
3. Use the following endpoints to generate data:
   - `/redis` - Sends data to Upstash Redis stream
   - `/dragonfly` - Sends data to DragonFly Redis stream
   - `/nats` - Sends data to NATS JetStream

## How It Works

The application uses a Durable Object (`StreamConsumer`) to maintain state between requests. When a request is made to the worker, it:

1. Creates or retrieves a Durable Object instance
2. The Durable Object connects to the configured streaming services
3. Depending on the implementation, it either:
   - Polls for new messages (Redis streams)
   - Uses consumer groups for distributed processing (DragonFly)
   - Subscribes to NATS JetStream for real-time updates

Each consumer implementation handles:
- Connection management
- Message retrieval
- Processing and acknowledgment
- Error handling and reconnection

## Configuration

Configuration is stored in `wrangler.toml` and environment variables. The application uses:
- Upstash Redis for cloud-based Redis streams
- DragonFly Redis for high-performance Redis alternative
- NATS for pub/sub messaging with JetStream for persistence